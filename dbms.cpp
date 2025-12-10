#include <iostream>
#include <fstream>
#include <string>
#include <filesystem>
#include <regex>
#include <random>
#include <chrono> // Для генерации ID
#include <cstdio> // Для sscanf и sprintf
#include "json.hpp"
#include "array.hpp"
#include "dh.hpp"

// Псевдоним для удобства
using json = nlohmann::json;
using namespace std;
random_device rd;

struct Timestamp {
    int year, month, day, hour, minute, second;

    // Конструктор из строки
    Timestamp(const string& ts) : year(0)
                                , month(0)
                                , day(0)
                                , hour(0)
                                , minute(0)
                                , second(0) {
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

// Функция-обертка для валидации в validateDocument
bool isValidTimestamp(const string& ts) {
    Timestamp t(ts);
    // Проверяем формат через паттерн и логическую валидность даты
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
            if (!arg.is_array()) return false;
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

    // Логические операторы верхнего уровня
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

    string insert(json document) {
        // Проверка схемы перед вставкой
        if (!validateDocument(document, structure)) {
            cerr << "Error: Document structure or types do not match the schema in collection '" << name << "'." << endl;
            return "";
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
        return id;
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
        // Получаем массив результатов
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
                        {"specs", {  // Вложенный объект
                            {"cpu", "str"},
                            {"ram", "int"},
                            {"screen", {  // Двойная вложенность
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

    string getName() const { return schemaName; }
    
    ~DBMS() {
        for (auto& kv : collections) delete kv.second;
    }
};

class ConsoleParser {
    DBMS& dbms;

    // Структура для хранения разобранных аргументов
    struct ParsedArgs {
        json arg1 = nullptr;  // query или document
        json arg2 = nullptr;  // updateOps или projection
        bool multi = false;   // флаг для update/delete
        bool hasArg2 = false;
    };

    // Безопасное разделение строки аргументов
    Array<string> splitArguments(string argsStr) {
        Array<string> args;
        string buffer;
        int balanceBrace = 0;   // {}
        int balanceBracket = 0; // []
        
        for (size_t i = 0; i < argsStr.length(); ++i) {
            char c = argsStr[i];
            
            if (c == '{') balanceBrace++;
            else if (c == '}') balanceBrace--;
            else if (c == '[') balanceBracket++;
            else if (c == ']') balanceBracket--;
            
            // Разделяем по запятой, только если мы не внутри объекта/массива
            if (c == ',' && balanceBrace == 0 && balanceBracket == 0) {
                // Триминг пробелов
                size_t first = buffer.find_first_not_of(" \t");
                size_t last = buffer.find_last_not_of(" \t");
                if (first != string::npos) {
                    args.push_back(buffer.substr(first, (last - first + 1)));
                }
                buffer = "";
            } else {
                buffer += c;
            }
        }
        // Добавляем последний аргумент
        size_t first = buffer.find_first_not_of(" \t");
        size_t last = buffer.find_last_not_of(" \t");
        if (first != string::npos) {
            args.push_back(buffer.substr(first, (last - first + 1)));
        }
        
        return args;
    }

    // Парсинг значений (True/False) и именованных параметров
    ParsedArgs parseArgsInternal(const Array<string>& rawArgs) {
        ParsedArgs res;
        
        for (uint32_t i = 0; i < rawArgs.GetSize(); ++i) {
            string current = rawArgs[i];
            
            // Обработка projection=
            if (current.rfind("projection=", 0) == 0) {
                string val = current.substr(11);
                try {
                    res.arg2 = json::parse(val);
                    res.hasArg2 = true;
                } catch (...) { cerr << "Invalid projection JSON" << endl; }
                continue;
            }
            
            // Обработка multi=True/False
            if (current.find("multi=") != string::npos) {
                if (current.find("True") != string::npos || current.find("true") != string::npos) {
                    res.multi = true;
                } else {
                    res.multi = false;
                }
                continue;
            }

            // Обычные JSON аргументы
            try {
                json j = json::parse(current);
                if (i == 0) res.arg1 = j;
                else if (i == 1) { res.arg2 = j; res.hasArg2 = true; }
            } catch (json::parse_error& e) {
                cerr << "JSON Parse Error at argument " << i+1 << ": " << e.what() << endl;
            }
        }
        return res;
    }

public:
    ConsoleParser(DBMS& db) : dbms(db) {}

    void execute(const string& commandLine) {
        // Защита от пустых строк
        if (commandLine.empty()) return;

        // Базовая валидация структуры: dbName.collName.method(args)
        regex cmdPattern(R"(^(\w+)\.(\w+)\.(\w+)\((.*)\)$)");
        smatch matches;
        
        if (!regex_match(commandLine, matches, cmdPattern)) {
            cerr << "Syntax Error. Expected: db.collection.method(args)" << endl;
            return;
        }

        string dbName = matches[1];
        string colName = matches[2];
        string method = matches[3];
        string argsStr = matches[4];

        // Проверка имени БД
        if (dbName != dbms.getName()) {
            cerr << "Error: Unknown database '" << dbName << "'" << endl;
            return;
        }

        // Получение коллекции
        Collection* col = dbms.getCollection(colName);
        if (!col) {
            cerr << "Error: Collection '" << colName << "' not found." << endl;
            return;
        }

        // Парсинг аргументов
        Array<string> rawArgs = splitArguments(argsStr);
        ParsedArgs parsed = parseArgsInternal(rawArgs);

        try {
            if (method == "find") {
                json res = col->find(parsed.arg1, parsed.arg2);
                if (res != nullptr) cout << res.dump(4) << endl;
                else cout << "null" << endl;
            }
            else if (method == "find_one") {
                json res = col->find_one(parsed.arg1, parsed.arg2);
                if (res != nullptr) cout << res.dump(4) << endl;
                else cout << "null" << endl;
            }
            else if (method == "insert") {
                if (parsed.arg1.is_null()) { cerr << "Insert requires a document." << endl; return; }
                string id = col->insert(parsed.arg1);
                if(!id.empty()) cout << "Inserted ID: " << id << endl;
            }
            else if (method == "insert_many") {
                if (parsed.arg1.is_null() || !parsed.arg1.is_array()) { cerr << "insert_many requires an array." << endl; return; }
                col->insert_many(parsed.arg1);
            }
            else if (method == "update") {
                if (parsed.arg1.is_null() || !parsed.hasArg2) { cerr << "Update requires query and update operators." << endl; return; }
                col->update(parsed.arg1, parsed.arg2, parsed.multi);
            }
            else if (method == "update_one") {
                if (parsed.arg1.is_null() || !parsed.hasArg2) { cerr << "Update requires query and update operators." << endl; return; }
                col->update(parsed.arg1, parsed.arg2, false);
            }
             else if (method == "update_many") {
                if (parsed.arg1.is_null() || !parsed.hasArg2) { cerr << "Update requires query and update operators." << endl; return; }
                col->update(parsed.arg1, parsed.arg2, true);
            }
            else if (method == "delete_one") {
                col->remove(parsed.arg1, false);
            }
            else if (method == "delete_many") {
                col->remove(parsed.arg1, true);
            }
            else {
                cerr << "Unknown method: " << method << endl;
            }
        } catch (exception& e) {
            cerr << "Execution Error: " << e.what() << endl;
        }
    }
};

int main() {
    setlocale(LC_ALL, "ru");
    
    // Инициализация СУБД с конфигурацией
    DBMS db("schema.json");
    ConsoleParser parser(db);

    cout << "DBMS initialized. Database: " << db.getName() << endl;
    cout << "Enter commands (e.g. " << db.getName() << ".users.find({})). Type 'exit' to quit." << endl;

    string line;
    while (true) {
        cout << "> ";
        if (!getline(cin, line)) break;
        if (line == "exit") break;
        
        // Пропуск пустых строк
        if (line.find_first_not_of(" \t\n\r") == string::npos) continue;

        parser.execute(line);
    }

    return 0;
}
