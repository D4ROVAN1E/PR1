#ifndef DH_HPP
#define DH_HPP

#include <iostream>
#include <cstdint>
#include <cmath>
#include <string>
#include <fstream> 
#include <stdexcept> 
#include "array.hpp"

using namespace std;

// Структура для хранения пары ключ-значение
template <typename T>
struct HashNode {
    string first;
    T second;
    bool isOccupied;

    HashNode() : first(""), second(T()), isOccupied(false) {}

    HashNode(const string& newKey, const T& newValue)
        : first(newKey), second(newValue), isOccupied(true) {
    }
};

template <typename T>
class DoubleHash {
 private:
    Array<HashNode<T>> table;
    uint32_t tableSize;        // Размер таблицы
    uint32_t elementsCount;    // Количество элементов
    // Дробная часть золотого сечения
    const double A = (sqrt(5.0) - 1.0) / 2.0;

    // Первая хэш-функция: метод умножения
    [[nodiscard]] auto hash1(const string& key) const -> uint32_t {
        // Преобразуем строку в число
        uint64_t numKey = 0;
        for (char c : key) {
            numKey = numKey * 31 + static_cast<uint64_t>(c);
        }

        // hash(k) = floor(M * ((k * A) mod 1))
        double temp = numKey * A;
        temp = temp - floor(temp);   // Получаем дробную часть
        return static_cast<uint32_t>(floor(tableSize * temp));
    }

    // Вторая хэш-функция: метод свёртки
    [[nodiscard]] auto hash2(const string& key) const -> uint32_t {
        // Разбиваем ключ на части и складываем
        uint32_t sum = 0;
        for (char c : key) {
            sum += static_cast<uint8_t>(c);
        }

        // Возвращаем нечётное число
        uint32_t result = (sum % (tableSize - 1)) + 1;

        // Делаем результат нечётным, если размер таблицы чётный
        if (tableSize % 2 == 0 && result % 2 == 0) {
            result++;
        }

        return result;
    }

    // Функция для проверки необходимости расширения таблицы
    [[nodiscard]] auto needResize() const -> bool {
        if (tableSize == 0) return true; // Защита от деления на ноль
        return (static_cast<double>(elementsCount) / tableSize) > 0.7;
    }

    // Расширение таблицы при достижении порога загрузки
    void resize() {
        uint32_t oldSize = tableSize;
        Array<HashNode<T>> oldTable = table;

        // Увеличиваем размер таблицы
        // Делаем нечётным для лучшего распределения
        tableSize = tableSize * 2 + 1;

        // Создаём новую таблицу
        table = Array<HashNode<T>>(tableSize + 1);
        for (uint32_t i = 0; i < tableSize; i++) {
            table[i] = HashNode<T>();
        }
        table.SetSize(tableSize);

        elementsCount = 0;

        // Перехэшируем все элементы
        for (uint32_t i = 0; i < oldSize; i++) {
            if (oldTable[i].isOccupied) {
                insert(oldTable[i].first, oldTable[i].second);
            }
        }
    }

 public:
    struct Iterator {
        Array<HashNode<T>>* tableRef;
        uint32_t index;
        uint32_t totalSize;

        Iterator(Array<HashNode<T>>* tbl, uint32_t startIdx, uint32_t size) : tableRef(tbl)
                                                                            , index(startIdx)
                                                                            , totalSize(size) {
            // Проматываем пустые ячейки при создании, если мы не в конце
            while (index < totalSize && !(*tableRef)[index].isOccupied) {
                index++;
            }
        }

        // Разыменование возвращает HashNode&, у которого есть поля first и second
        HashNode<T>& operator*() { return (*tableRef)[index]; }
        HashNode<T>* operator->() { return &((*tableRef)[index]); }

        Iterator& operator++() {
            do {
                index++;
            } while (index < totalSize && !(*tableRef)[index].isOccupied);
            return *this;
        }

        bool operator!=(const Iterator& other) const {
            return index != other.index;
        }

        bool operator==(const Iterator& other) const {
            return index == other.index;
        }
    };

    Iterator begin() { return Iterator(&table, 0, tableSize); }

    Iterator end() { return Iterator(&table, tableSize, tableSize); }

    // Конструктор
    explicit DoubleHash(uint32_t size = 3) : tableSize(size)
                                    , elementsCount(0)
                                    , table(Array<HashNode<T>>(size + 1)) {
        if (size == 0) {
            throw invalid_argument("Table size cannot be zero");
        }
        for (uint32_t i = 0; i < tableSize; i++) {
            table[i] = HashNode<T>();
        }
        table.SetSize(tableSize);
    }

    // Деструктор
    ~DoubleHash() {
        // Array имеет свой деструктор, который освободит память
    }

    // Копирующий конструктор
    DoubleHash(const DoubleHash<T>& other) : tableSize(other.tableSize)
                            , elementsCount(other.elementsCount)
                            , table(Array<HashNode<T>>(other.tableSize + 1)) {
        // Копируем все элементы таблицы
        for (uint32_t i = 0; i < tableSize; i++) {
            table[i] = other.table[i];
        }
        table.SetSize(tableSize);
    }

    // Копирующий оператор присваивания
    auto operator=(const DoubleHash<T>& other) -> DoubleHash<T>& {
        // Защита от самоприсваивания
        if (this == &other) {
            return *this;
        }

        // Копируем данные из other
        tableSize = other.tableSize;
        elementsCount = other.elementsCount;

        // Создаём новую таблицу нужного размера
        table = Array<HashNode<T>>(tableSize + 1);

        // Копируем все элементы
        for (uint32_t i = 0; i < tableSize; i++) {
            table[i] = other.table[i];
        }
        table.SetSize(tableSize);

        return *this;
    }

    T& operator[](const string& key) {
        Iterator it = find(key);
        if (it != end()) {
            return it->second;
        }
        // Если элемента нет, вставляем пустой и возвращаем ссылку на него
        insert(key, T());
        return find(key)->second;
    }

    // Вставка элемента
    void insert(const string& key, const T& value) {
        if (needResize()) {
            resize();
        }

        uint32_t h1 = hash1(key);
        uint32_t h2 = hash2(key);
        uint32_t i = 0;

        while (i < tableSize) {
            uint32_t index = (h1 + i * h2) % tableSize;

            // Если ячейка свободна или была удалена, вставляем
            if (!table[index].isOccupied) {
                table[index] = HashNode<T>(key, value);
                elementsCount++;
                return;
            }

            // Если ключ уже существует, обновляем значение
            if (table[index].first == key) {
                table[index].second = value;
                return;
            }

            i++;
        }

        // Если мы здесь, значит не удалось вставить элемент (таблица забита или проблема хэш-функции)
        throw overflow_error("Error: Hash table is full, cannot insert key.");
    }

    // Поиск элемента по ключу
    auto find(const string& key) -> Iterator {
        if (elementsCount == 0) return end();

        uint32_t h1 = hash1(key);
        uint32_t h2 = hash2(key);
        uint32_t i = 0;

        while (i < tableSize) {
            uint32_t index = (h1 + i * h2) % tableSize;

            // Если ячейка никогда не использовалась, элемента нет
            if (!table[index].isOccupied) {
                return end();
            }

            // Если нашли ключ и элемент не удалён
            if (table[index].first == key && table[index].isOccupied) {
                return Iterator(&table, index, tableSize);
            }

            i++;
        }

        return end();
    }

    // Удаление элемента
    auto remove(const string& key) -> bool {
        if (elementsCount == 0) return false;

        uint32_t h1 = hash1(key);
        uint32_t h2 = hash2(key);
        uint32_t i = 0;

        while (i < tableSize) {
            uint32_t index = (h1 + i * h2) % tableSize;

            if (!table[index].isOccupied) {
                return false;
            }

            if (table[index].first == key && table[index].isOccupied) {
                table[index].isOccupied = false;
                elementsCount--;
                return true;
            }

            i++;
        }

        return false;
    }

    // Печать таблицы
    void print() const {
        cout << "=== Хэш-таблица ===" << endl;
        cout << "Размер: " << tableSize
        << ", Элементов: " << elementsCount << endl;
        for (uint32_t i = 0; i < tableSize; i++) {
            if (table[i].isOccupied) {
                cout << "[" << i << "] " << table[i].first << " => " << table[i].second << endl;
            }
        }
        cout << "===================" << endl;
    }
    
    // Сериализация в текстовом формате
    void serialize_text(const string& filename) const {
        ofstream outFile(filename);
        if (!outFile.is_open()) {
            throw runtime_error("Error: Could not open file for writing: " + filename);
        }

        // Записываем заголовок
        outFile << tableSize << " " << elementsCount << endl;

        // Записываем только занятые ячейки
        for (uint32_t i = 0; i < tableSize; i++) {
            if (table[i].isOccupied) {
                outFile << i << " " << table[i].first << " " << table[i].second << endl;
            }
        }

        outFile.close();
        cout << "Таблица (текст) успешно сохранена в " << filename << endl;
    }

    // Десериализация из текстового формата
    void deserialize_text(const string& filename) {
        ifstream inFile(filename);
        if (!inFile.is_open()) {
            throw runtime_error("Error: Could not open file for reading: " + filename);
        }

        uint32_t newTableSize = 0;
        uint32_t newElementsCount = 0;

        // Читаем заголовок
        inFile >> newTableSize >> newElementsCount;
        if (newTableSize == 0)
            throw runtime_error("Could not read data from file. Size of table equal to zero");

        // Пересоздаем таблицу
        try {
            table = Array<HashNode<T>>(newTableSize + 1);
            for (uint32_t i = 0; i < newTableSize; i++) {
                table[i] = HashNode<T>();
            }
            table.SetSize(newTableSize);
        } catch (...) {
            throw runtime_error("Error: Memory allocation failed during deserialization");
        }

        tableSize = newTableSize;
        elementsCount = newElementsCount;

        // Читаем данные
        uint32_t idx;
        string key;
        T value;

        // Читаем данные
        while (inFile >> idx >> key >> value) {
            
            if (inFile.fail()) {
                 throw runtime_error("Error: Corrupted data in file: " + filename);
            }

            if (idx >= tableSize) {
                throw out_of_range("Error: Index in file (" + to_string(idx) + 
                                        ") exceeds table size (" + to_string(tableSize) + ")");
            }
            table[idx] = HashNode<T>(key, value);
        }

        inFile.close();
        cout << "Таблица (текст) успешно загружена из " << filename << endl;
    }

    // Сериализация в бинарном формате
    void serialize_bin(const string& filename) const {
        ofstream outFile(filename, ios::binary);
        if (!outFile.is_open()) {
            throw runtime_error("Error: Could not open binary file for writing: " + filename);
        }

        // Записываем размер таблицы и количество элементов
        outFile.write(reinterpret_cast<const char*>(&tableSize), sizeof(tableSize));
        outFile.write(reinterpret_cast<const char*>(&elementsCount), sizeof(elementsCount));

        for (uint32_t i = 0; i < tableSize; i++) {
            bool occupied = table[i].isOccupied;
            outFile.write(reinterpret_cast<const char*>(&occupied), sizeof(bool));

            if (occupied) {
                // Записываем длину ключа
                uint32_t keyLen = static_cast<uint32_t>(table[i].first.size());
                outFile.write(reinterpret_cast<const char*>(&keyLen), sizeof(keyLen));

                // Записываем сам ключ
                outFile.write(table[i].first.c_str(), keyLen);

                // Записываем значение
                outFile.write(reinterpret_cast<const char*>(&table[i].second), sizeof(T));
            }
        }

        outFile.close();
        cout << "Таблица (бинарн.) успешно сохранена в " << filename << endl;
    }

    // Десериализация из бинарного формата
    void deserialize_bin(const string& filename) {
        ifstream inFile(filename, ios::binary);
        if (!inFile.is_open()) {
            throw runtime_error("Error: Could not open binary file for reading: " + filename);
        }

        // Читаем размеры
        uint32_t newTableSize = 0;
        uint32_t newElementsCount = 0;

        inFile.read(reinterpret_cast<char*>(&newTableSize), sizeof(newTableSize));
        inFile.read(reinterpret_cast<char*>(&newElementsCount), sizeof(newElementsCount));

        table = Array<HashNode<T>>(newTableSize + 1);
        for (uint32_t i = 0; i < newTableSize; i++) {
            table[i] = HashNode<T>();
        }
        table.SetSize(newTableSize);

        tableSize = newTableSize;
        elementsCount = newElementsCount;

        // Читаем данные ячеек
        for (uint32_t i = 0; i < tableSize; i++) {
            bool occupied = false;
            inFile.read(reinterpret_cast<char*>(&occupied), sizeof(bool));

            if (inFile.fail()) {
                throw runtime_error("Error: Unexpected end of file or read error");
            }

            if (occupied) {
                // Читаем длину ключа
                uint32_t keyLen = 0;
                inFile.read(reinterpret_cast<char*>(&keyLen), sizeof(keyLen));

                // Защита от переполнения памяти при чтении длины строки
                if (keyLen > 1000000) { // Разумный лимит
                    throw length_error("Error: Key length in file seems too large (corrupted file?)");
                }

                char* keyBuf = new char[keyLen + 1];
                inFile.read(keyBuf, keyLen);
                
                if (inFile.fail()) {
                    delete[] keyBuf;
                    throw runtime_error("Error: Failed to read key string");
                }
                
                keyBuf[keyLen] = '\0';
                string loadedKey(keyBuf);
                delete[] keyBuf;

                // Читаем значение
                T loadedValue;
                inFile.read(reinterpret_cast<char*>(&loadedValue), sizeof(T));

                if (inFile.fail()) {
                    throw runtime_error("Error: Failed to read value");
                }

                table[i] = HashNode<T>(loadedKey, loadedValue);
            } else {
                table[i].isOccupied = false;
            }
        }

        inFile.close();
        cout << "Таблица (бинарн.) успешно загружена из " << filename << endl;
    }

    // Получение количества элементов
    [[nodiscard]] auto size() const -> uint32_t {
        return elementsCount;
    }

    // Проверка на пустоту
    [[nodiscard]] auto empty() const -> bool {
        return elementsCount == 0;
    }

    // Очистка таблицы
    void clear() {
        for (uint32_t i = 0; i < tableSize; i++) {
            table[i] = HashNode<T>();
        }
        elementsCount = 0;
    }
};

#endif   // DH_HPP
