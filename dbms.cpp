#include <iostream>
#include <fstream>
#include <string>
#include <filesystem>
#include <regex>
#include <random>
#include <chrono> // Добавлено для генерации ID
#include <cstdio> // для sscanf и sprintf
#include "json.hpp"
#include "array.hpp"
#include "dh.hpp"

// Используем псевдоним для удобства
using json = nlohmann::json;
using namespace std;
random_device rd;

struct Timestamp {
    int year, month, day, hour, minute, second;

    // Конструктор из строки
    Timestamp(const string& ts) {
        if (sscanf(ts.c_str(), "%d-%d-%dT%d:%d:%d", &year, &month, &day, &hour, &minute, &second) != 6) { // Если успешно записаны не 6
            cerr << "Could't parse timestamp data" << endl;
        }
    }

    // Проверка на високосный год
    bool isLeap(int y) const {
        return (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
    }

    // Количество дней в месяце
    int daysInMonth(int m, int y) const {
        static const int days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        if (m == 2 && isLeap(y)) return 29;
        return days[m];
    }

    // Основная логика инкремента
    void addSeconds(int secToAdd) {
        second += secToAdd;

        // Нормализация секунд
        while (second >= 60) {
            second -= 60;
            minute++;
        }
        // Нормализация минут
        while (minute >= 60) {
            minute -= 60;
            hour++;
        }
        // Нормализация часов
        while (hour >= 24) {
            hour -= 24;
            day++;
        }

        // Нормализация дней (переход через месяцы и годы)
        while (true) {
            int dim = daysInMonth(month, year);
            if (day <= dim) break; // Если день вписывается в месяц — выходим
            
            day -= dim; // Вычитаем дни текущего месяца
            month++;    // Переходим к следующему
            
            if (month > 12) { // Новый год
                month = 1;
                year++;
            }
        }
    }

    // Конвертация обратно в строку
    string toString() const {
        char buffer[25];
        // %02d добавляет ведущий ноль, если число меньше 10
        sprintf(buffer, "%04d-%02d-%02dT%02d:%02d:%02d", year, month, day, hour, minute, second);
        return string(buffer);
    }
    
    // Валидация 
    bool isValid() const {
        if (month < 1 || month > 12) return false;
        if (day < 1 || day > daysInMonth(month, year)) return false;
        if (hour < 0 || hour > 23) return false;
        if (minute < 0 || minute > 59) return false;
        if (second < 0 || second > 59) return false;
        return true;
    }
};

// Функция-обертка для валидации в Collection::validateDocument
bool isValidTimestamp(const string& ts) {
    Timestamp t(ts);
    // Проверяем формат через регулярку И логическую валидность даты
    static const regex pattern(R"(^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$)");
    return regex_match(ts, pattern) && t.isValid();
}

// Предварительное объявление
bool matchDocument(const json& doc, const json& query);

bool checkCondition(const json& value, const json& condition) {
    if (!condition.is_object()) {
        return value == condition; 
    }

    bool isOperatorQuery = false;
    for (auto& [key, val] : condition.items()) {
        if (key[0] == '$') {
            isOperatorQuery = true;
            break;
        }
    }

    if (!isOperatorQuery) {
        if (value.is_object()) {
            return matchDocument(value, condition);
        }
        return value == condition;
    }

    for (auto& [op, arg] : condition.items()) {
        if (op == "$eq") { if (value != arg) return false; }
        else if (op == "$ne") { if (value == arg) return false; }
        else if (op == "$gt") { if (value <= arg) return false; }
        else if (op == "$lt") { if (value >= arg) return false; }
        else if (op == "$gte") { if (value < arg) return false; }
        else if (op == "$lte") { if (value > arg) return false; }
        else if (op == "$in") { 
            bool found = false;
            for (const auto& item : arg) {
                if (item == value) { found = true; break; }
            }
            if (!found) return false;
        }
        else if (op == "$not") { 
            if (checkCondition(value, arg)) return false; 
        }
    }
    return true;
}

bool matchDocument(const json& doc, const json& query) {
    if (query.empty()) return true;

    // Логические операторы верхнего уровня ($and, $or)
    if (query.contains("$and")) {
        for (const auto& subQuery : query["$and"]) {
            if (!matchDocument(doc, subQuery)) return false;
        }
        return true;
    }
    if (query.contains("$or")) {
        for (const auto& subQuery : query["$or"]) {
            if (matchDocument(doc, subQuery)) return true;
        }
        return false;
    }

    // Проверка полей
    for (auto& [key, condition] : query.items()) {
        if (key[0] == '$') continue; 

        if (!doc.contains(key)) {
             if (!checkCondition(nullptr, condition)) return false;
        } else {
             if (!checkCondition(doc[key], condition)) return false;
        }
    }
    return true;
}

class Collection {
    string name;
    string path;
    size_t tuples_limit;
    json structure; 

    string generateId() {
        mt19937 gen(rd());
        return to_string(chrono::system_clock::now().time_since_epoch().count()) + "_" + to_string(gen());
    }

    Array<int> getFileIndexes() {
        Array<int> indexes;
        if (!filesystem::exists(path)) return {1};
        
        for (const auto& entry : filesystem::directory_iterator(path)) {
            string fname = entry.path().filename().string();
            if (fname.find(".json") != string::npos) {
                try {
                    indexes.push_back(stoi(fname.substr(0, fname.find("."))));
                } catch (...) {
                    cerr << "Couldn't read json index for " << fname << " skipping..." << endl;
                }
            }
        }
        if (indexes.empty()) indexes.push_back(1);
        sort(indexes.begin(), indexes.end());
        return indexes;
    }

    bool validateDocument(const json& doc, const json& schemaSubset) {
        for (auto& [key, typeVal] : schemaSubset.items()) {
            // Проверяем только те поля, которые есть и в документе, и в схеме
            if (doc.contains(key)) {
                // Если в схеме это вложенный объект (например, "specs")
                if (typeVal.is_object()) {
                    if (!doc[key].is_object()) return false;
                    // Рекурсивная проверка вложенности
                    if (!validateDocument(doc[key], typeVal)) return false;
                } 
                // Если в схеме это строка с названием типа
                else if (typeVal.is_string()) {
                    string type = typeVal.get<string>();
                    
                    if (type == "int") {
                        if (!doc[key].is_number_integer()) return false;
                    } 
                    else if (type == "string" || type == "str") {
                        if (!doc[key].is_string()) return false;
                    } 
                    else if (type == "timestamp") {
                        // Проверяем, что это строка И она соответствует формату
                        if (!doc[key].is_string()) return false;
                        if (!isValidTimestamp(doc[key].get<string>())) return false;
                    }
                }
            }
        }
        return true;
    }

public:
    Collection(string newName, string newPath, size_t limit, json initialStructure) 
                                                                                : name(newName),
                                                                                path(newPath),
                                                                                tuples_limit(limit),
                                                                                structure(initialStructure)
    {
        if (!filesystem::exists(path)) {
            filesystem::create_directories(path);
            ofstream out(path + "/1.json");
            out << "{}";
            out.close();
        }
    }

    void insert(json document) {
        // Проверка схемы перед вставкой
        if (!validateDocument(document, structure)) {
            cerr << "Error: Document structure or types do not match the schema in collection '" << name << "'." << endl;
            return;
        }

        string id;
        if (document.contains("_id")) id = document["_id"];
        else id = generateId(); 
        document["_id"] = id; 

        auto indexes = getFileIndexes();
        int lastIdx = indexes.back();
        string filePath = path + "/" + to_string(lastIdx) + ".json";
        
        json fileData;
        if (filesystem::exists(filePath) && filesystem::file_size(filePath) > 0) {
            ifstream in(filePath);
            try { in >> fileData; } catch(...) {
                fileData = json::object();
                cerr << "Couldn't read file data from " << filePath << " creating empty json..." << endl;
            }
            in.close();
        } else {
            fileData = json::object();
        }

        if (fileData.size() >= tuples_limit) {
            filePath = path + "/" + to_string(++lastIdx) + ".json";
            fileData = json::object();
        }

        fileData[id] = document; 

        ofstream out(filePath);
        out << fileData.dump(4);
        out.close();
    }

    void insert_one(const json& document) {
        if (document.is_array()) {
            cerr << "Expected one document" << endl;
            return;
        }
        insert(document);
    }

    void insert_many(const json& documents) {
        if (!documents.is_array()) {
            cerr << "insert_many expects an array of documents" << endl;
            return;
        }
        for (const auto& doc : documents) {
            insert(doc);
        }
    }

    json find(const json& query, const json& projection = nullptr, bool findOne = false) {
        json result = json::array();
        auto indexes = getFileIndexes();

        for (int idx : indexes) {
            ifstream in(path + "/" + to_string(idx) + ".json");
            json chunk;
            if (in.good()) {
                try { in >> chunk; } catch(...) {
                    cerr << "Couldn't read file data from " << path + "/" + to_string(idx) + ".json Skipping..." << endl;
                    continue;
                }
            }
            in.close();

            for (auto& [key, doc] : chunk.items()) {
                if (matchDocument(doc, query)) {
                    if (projection != nullptr && !projection.empty()) {
                        json projectedDoc;
                        // Если проекция - массив ключей ["name", "age"]
                        if (projection.is_array()) {
                             for(const auto& field : projection) {
                                if(doc.contains(field)) projectedDoc[field] = doc[field];
                             }
                        } 
                        // Если проекция - объект {"name": 1}
                        else if (projection.is_object()) {
                            for (auto& [pKey, pVal] : projection.items()) {
                                if (doc.contains(pKey)) projectedDoc[pKey] = doc[pKey];
                            }
                        }
                        result.push_back(projectedDoc);
                    } else {
                        result.push_back(doc);
                    }
                    if (findOne) return result;
                }
            }
        }
        return result;
    }

    json find_one(const json& query, const json& projection = nullptr) {
        // Получаем массив результатов (максимум 1 элемент благодаря true)
        json result = find(query, projection, true);
        
        // Если массив пуст - документ не найден
        if (result.empty()) {
            return nullptr; 
        }
        
        // Возвращаем первый элемент массива
        return result[0];
    }

    void update(const json& query, const json& updateOps, bool multi = false) {
        auto indexes = getFileIndexes();
        bool updatedOne = false;

        for (int idx : indexes) {
            if (!multi && updatedOne) break; 

            string fpath = path + "/" + to_string(idx) + ".json";
            ifstream in(fpath);
            json chunk; 
                if (in.good()) {
                    try { in >> chunk; } catch(...) {
                    cerr << "Couldn't read file data from " << path + "/" + to_string(idx) + ".json Skipping..." << endl;
                    continue;
                }
            }
            in.close();

            bool fileChanged = false;
            for (auto& [key, doc] : chunk.items()) {
                if (matchDocument(doc, query)) {
                    // Обработка $set
                    if (updateOps.contains("$set")) {
                        for (auto& [k, v] : updateOps["$set"].items()) doc[k] = v;
                    }
                    // Обработка $inc
                    if (updateOps.contains("$inc")) {
                        for (auto& [k, v] : updateOps["$inc"].items()) {
                            if (doc.contains(k)) {
                                // Определяем тип из схемы
                                string fieldType = "";
                                if (structure.contains(k) && structure[k].is_string()) {
                                    fieldType = structure[k].get<string>();
                                }

                                // Логика для Timestamp
                                if (fieldType == "timestamp") {
                                    // Создаем структуру из текущей строки
                                    Timestamp ts(doc[k].get<string>());
                                    
                                    // Прибавляем секунды
                                    ts.addSeconds(v.get<int>());
                                    
                                    // Записываем обратно строку
                                    doc[k] = ts.toString();
                                } 
                                // Логика для обычных чисел
                                else {
                                    doc[k] = doc[k].get<int>() + v.get<int>();
                                }
                            }
                        }
                    }
                    // Обработка $push
                    if (updateOps.contains("$push")) { 
                         for (auto& [k, v] : updateOps["$push"].items()) {
                             if (!doc.contains(k)) doc[k] = json::array();
                             doc[k].push_back(v);
                         }
                    }
                    
                    fileChanged = true;
                    updatedOne = true;
                    if (!multi) break; 
                }
            }

            if (fileChanged) {
                ofstream out(fpath);
                out << chunk.dump(4);
                out.close();
            }
        }
    }

    void update_one(const json& query, const json& updateOps) {
        update(query, updateOps, false);
    }

    void update_many(const json& query, const json& updateOps) {
        update(query, updateOps, true);
    }

    void remove(const json& query, bool multi = false) {
        auto indexes = getFileIndexes();
        bool deletedOne = false;

        for (int idx : indexes) {
            if (!multi && deletedOne) break;

            string fpath = path + "/" + to_string(idx) + ".json";
            ifstream in(fpath);
            json chunk;
                if (in.good()) {
                    try { in >> chunk; } catch(...) {
                    cerr << "Couldn't read file data from " << path + "/" + to_string(idx) + ".json Skipping..." << endl;
                    continue;
                }
            }
            in.close();

            Array<string> keysToDelete;
            for (auto& [key, doc] : chunk.items()) {
                if (matchDocument(doc, query)) {
                    keysToDelete.push_back(key);
                    deletedOne = true;
                    if (!multi) break; 
                }
            }

            if (!keysToDelete.empty()) {
                for(const auto& k : keysToDelete) chunk.erase(k);
                ofstream out(fpath);
                out << chunk.dump(4); 
                out.close();
            }
        }
    }

    void delete_one(const json& query) {
        remove(query, false);
    }

    void delete_many(const json& query) {
        remove(query, true);
    }
};

class DBMS {
    string schemaName;
    string configPath;
    size_t tuplesLimit;
    DoubleHash<Collection*> collections;

public:
    DBMS(const string& cfgPath) : configPath(cfgPath) {
        ifstream f(configPath);
        if (!f.is_open()) {
            cout << "Config file not found. Creating default schema with NESTED structures..." << endl;
            json defaultSchema = {
                {"name", "MyDatabase"},
                {"tuples_limit", 5},
                {"structure", {
                    {"users", {
                        {"name", "str"},
                        {"age", "int"},
                        {"status", "str"},
                        {"score", "int"},
                        {"hunted", "timestamp"}
                    }},
                    // Коллекция с вложенной структурой
                    {"products", {
                        {"name", "str"},
                        {"specs", {             // Вложенный объект
                            {"cpu", "str"},
                            {"ram", "int"},
                            {"screen", {        // Двойная вложенность
                                {"size", "int"},
                                {"type", "str"}
                            }}
                        }}
                    }}
                }}
            };
            ofstream out(configPath);
            out << defaultSchema.dump(4);
            out.close();
            f.open(configPath);
        }
        
        json config;
        try { f >> config; } catch(...) {
            cerr << "Couldn't read schema from " << configPath << endl;
            return;
        }
        
        schemaName = config["name"];
        tuplesLimit = config["tuples_limit"];

        if (!filesystem::exists(schemaName)) {
            filesystem::create_directory(schemaName);
        }

        for (auto& [colName, schemaStruct] : config["structure"].items()) {
            string colPath = schemaName + "/" + colName;
            collections[colName] = new Collection(colName, colPath, tuplesLimit, schemaStruct);
        }
    }
    
    Collection* getCollection(const string& name) {
        if (collections.find(name) != collections.end()) {
            return collections[name];
        }
        return nullptr;
    }
    
    ~DBMS() {
        for (auto& kv : collections) delete kv.second;
    }
};

int main() {
    setlocale(LC_ALL, "ru");
    // Инициализация СУБД с конфигурацией
    DBMS db("schema.json");

    // Используем коллекцию 'users', так как большинство примеров в методичке связано с ней
    Collection* users = db.getCollection("users");
    if (!users) {
        cerr << "Collection users not found" << endl;
        return 1;
    }

    // Очистка коллекции перед началом тестов
    users->delete_many({});

    cout << "=== 1. INSERT (Вставка) ===" << endl;
    
    // 1.1 insert_one (вставка одного документа) [cite: 28, 30]
    cout << "Inserting Alice..." << endl;
    users->insert({
        {"name", "alice"},
        {"age", 25},
        {"status", "active"},
        {"score", 45},
        {"hunted", "2025-02-12T12:00:30"}
    });

    // 1.2 insert_many (вставка нескольких документов) [cite: 6, 31, 33]
    cout << "Inserting Bob, Carol, Dave, Eve..." << endl;
    users->insert_many({
        {
            {"user", "bob"}, 
            {"age", 30}, 
            {"status", "fail"}, 
            {"score", 20},
            {"priority", "high"},
            {"hunted", "2025-13-12T12:00:30"}
        },
        {
            {"name", "carol"}, 
            {"age", 20}, 
            {"status", "warn"}, 
            {"score", 95},
            {"hunted", "hi"}
        },
        {
            {"name", "dave"}, 
            {"age", 40}, 
            {"status", "active"}, 
            {"score", 100}
        },
        {
            {"name", "eve"}, 
            {"age", 15}, 
            {"status", "obsolete"}, 
            {"score", 0}
        }
    });

    users->insert({
        {"name", "ivan"},
        {"age", 60},
        {"status", "best_worker"},
        {"score", 2000}
    });
    
    // Вывод всех документов [cite: 14, 15]
    cout << "All users: " << users->find({}).dump(4) << endl;


    cout << "\n=== 2. FIND (Поиск и Операторы сравнения) ===" << endl;

    // 2.1 Простой фильтр (равно) [cite: 17, 42]
    cout << "Find status='fail':" << endl;
    cout << users->find({{"status", "fail"}}).dump(4) << endl;

    // 2.2 Операторы сравнения $gt, $lt [cite: 19, 42]
    cout << "Find score > 90 ($gt):" << endl;
    cout << users->find({{"score", {{"$gt", 90}}}}).dump(4) << endl;

    cout << "Find age < 25 ($lt):" << endl;
    cout << users->find({{"age", {{"$lt", 25}}}}).dump(4) << endl;

    // 2.3 Оператор $in (входит в список) [cite: 20, 42]
    cout << "Find status in ['error', 'warn'] ($in):" << endl;
    cout << users->find({{"status", {{"$in", {"error", "warn"}}}}}).dump(4) << endl;

    // 2.4 Оператор $ne (не равно) [cite: 42]
    cout << "Find user != 'alice' ($ne):" << endl;
    // Ограничим вывод 1 элементом через find_one для краткости, проверяя логику
    cout << users->find_one({{"name", {{"$ne", "alice"}}}}) << endl;


    cout << "\n=== 3. LOGICAL OPERATORS (Логические операторы) ===" << endl;

    // 3.1 Оператор $and [cite: 22, 44]
    // Найти тех, у кого status=fail И priority=high
    cout << "Find status='fail' AND priority='high' ($and):" << endl;
    cout << users->find({
        {"$and", {
            {{"status", "fail"}},
            {{"priority", "high"}}
        }}
    }).dump(4) << endl;

    // 3.2 Оператор $or [cite: 44]
    // Найти тех, у кого возраст < 20 ИЛИ возраст > 35
    cout << "Find age < 20 OR age > 35 ($or):" << endl;
    cout << users->find({
        {"$or", {
            {{"age", {{"$lt", 20}}}},
            {{"age", {{"$gt", 35}}}}
        }}
    }).dump(4) << endl;

    // 3.3 Оператор $not [cite: 44]
    // Найти тех, у кого возраст НЕ больше 30 (т.е. <= 30)
    cout << "Find age NOT > 30 ($not):" << endl;
    cout << users->find({
        {"age", {{"$not", {{"$gt", 30}}}}}
    }).dump(4) << endl;


    cout << "\n=== 4. PROJECTION (Проекция) ===" << endl;
    
    // 4.1 Проекция полей [cite: 13, 23, 24]
    // Вывести только поля "user" и "status" для пользователя "alice"
    cout << "Projection ['name', 'status'] for 'alice':" << endl;
    cout << users->find({{"name", "alice"}}, {"name", "status"}).dump(4) << endl;


    cout << "\n=== 5. FIND_ONE (Найти один) ===" << endl;
    
    // 5.1 Получение одного документа [cite: 25, 27]
    cout << "Find one user with score=100:" << endl;
    cout << users->find_one({{"score", 100}}).dump(4) << endl;


    cout << "\n=== 6. UPDATE (Обновление) ===" << endl;

    // 6.1 $set (установить значение) [cite: 36]
    cout << "Update 'alice': set status='super_active' ($set)..." << endl;
    users->update_one(
        {{"name", "alice"}}, 
        {{"$set", {{"status", "super_active"}}}}
    );
    cout << "Alice after update: " << users->find_one({{"name", "alice"}}).dump(4) << endl;

    // 6.2 $inc (инкремент) [cite: 36]
    // Увеличить score на 10 для всех, у кого score < 50
    cout << "Update many: increment score by 10 where score < 50 ($inc)..." << endl;
    users->update_many(
        {{"score", {{"$lt", 50}}}},
        {{"$inc", {{"score", 10}}}}
    );
    // Проверим Боба (было 20 -> станет 30) и Алису (было 45 -> станет 55)
    cout << "Bob score: " << users->find_one({{"name", "bob"}}, {"score"}) << endl;
    cout << "Alice score: " << users->find_one({{"name", "alice"}})["score"] << endl;

    // 6.3 $push (добавление в массив) [cite: 9]
    cout << "Update 'alice': push 'login' to tags ($push)..." << endl;
    users->update_one(
        {{"name", "alice"}},
        {{"$push", {{"tags", "login"}}}}
    );
    cout << "Alice tags: " << users->find_one({{"name", "alice"}})["tags"] << endl;


    cout << "\n=== 7. DELETE (Удаление) ===" << endl;

    // 7.1 delete_one (удалить одного) [cite: 10, 39]
    cout << "Deleting 'alice' (delete_one)..." << endl;
    users->delete_one({{"name", "alice"}});
    if (users->find_one({{"user", "alice"}}) == nullptr) {
        cout << "Alice deleted successfully." << endl;
    }

    // 7.2 delete_many (удалить много по фильтру) [cite: 10, 40]
    cout << "Deleting all users with status='obsolete' (delete_many)..." << endl;
    users->delete_many({{"status", "obsolete"}}); // Удалит 'eve'
    
    // Проверка, что 'eve' удалена, а 'dave' (status: active) остался
    cout << "Eve found? " << (users->find_one({{"name", "eve"}}) != nullptr) << endl;
    cout << "Dave found? " << (users->find_one({{"name", "dave"}}) != nullptr) << endl;

    return 0;
}