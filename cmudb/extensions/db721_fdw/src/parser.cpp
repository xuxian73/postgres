#undef vsnprintf
#undef snprintf
#undef vsprintf
#undef sprintf
#undef vfprintf
#undef fprintf
#undef vprintf
#undef printf
#undef gettext
#undef dgettext
#undef ngettext
#undef dngettext

// #include "dog.h"
#include "fstream"
#include "string.h"
#include <vector>
#include <iostream>

struct BlockStats
{
    int32_t num; //"num": the number of values in this block (int)
    union
    {
        float f;
        int32_t i;
    } max_value;
    union
    {
        float f;
        int32_t i;
    } min_value;
    int32_t min_len = 0;
    int32_t max_len = 0;
    std::string max_value_str;
    std::string min_value_str;
};

struct db721_Column
{
    std::string name;
    std::string type;
    // the offset in the file for the first block of this column
    int32_t start_offset;
    // the number of blocks for this column
    int32_t num_blocks;
    std::vector<BlockStats> block_stats;
};

struct MetaData
{
    std::string table;
    int32_t max_value_per_block;
    int32_t num_columns;
    std::vector<db721_Column> columns;
};

inline void eatWhiteSpace(char *buf, int &pos)
{
    while (buf[pos] == ' ')
    {
        pos++;
    }
}

inline void eatString(char *buf, std::string str, int &pos)
{
#ifdef db721_DEBUG
    std::cout << "Eat: " << str << " pos: " << pos << std::endl;
#endif
    for (size_t i = 0; i < str.length(); i++)
    {
        if (buf[pos] != str[i])
        {
            std::cout << "Error: " << str << " expected, pos: " << pos << std::endl;
        }
        pos++;
    }
}

inline void getString(char *buf, std::string *str, int &pos)
{
    str->clear();
    while (buf[pos] != '"')
    {
        str->push_back(buf[pos]);
        pos++;
    }
}

inline void getInt(char *buf, int *num, int &pos)
{
    std::string tmp;
    while ('0' <= buf[pos] && buf[pos] <= '9')
    {
        tmp.push_back(buf[pos]);
        pos++;
    }
    *num = std::stoi(tmp);
}

inline void getFloat(char *buf, float *num, int &pos)
{
    std::string tmp;
    while (('0' <= buf[pos] && buf[pos] <= '9') || buf[pos] == '.')
    {
        tmp.push_back(buf[pos]);
        pos++;
    }
    *num = std::stof(tmp);
}

inline void eatComma(char *buf, int &pos)
{
    if (buf[pos] == ',')
    {
        pos++;
    }
    eatWhiteSpace(buf, pos);
}

inline void eatStrAttr(char *buf, std::string attr, std::string *str, int &pos)
{
    eatString(buf, "\"" + attr + "\": \"", pos);
    getString(buf, str, pos);
    eatString(buf, "\"", pos);
    eatComma(buf, pos);
}

inline void eatIntAttr(char *buf, std::string attr, int *num, int &pos)
{
    eatString(buf, "\"" + attr + "\": ", pos);
    getInt(buf, num, pos);
    eatComma(buf, pos);
}

inline void eatFloatAttr(char *buf, std::string attr, float *num, int &pos)
{
    eatString(buf, "\"" + attr + "\": ", pos);
    std::string tmp;
    getFloat(buf, num, pos);
    eatComma(buf, pos);
}

void eatTable(char *buf, int &pos, MetaData *metaData)
{
    eatStrAttr(buf, "Table", &metaData->table, pos);
}

void eatBlockStat(char *buf, int &pos, std::string &type, BlockStats *blockStat)
{
    if (type == "float")
    {
        eatFloatAttr(buf, "min", &blockStat->min_value.f, pos);
        eatFloatAttr(buf, "max", &blockStat->max_value.f, pos);
    }
    else if (type == "int")
    {
        eatIntAttr(buf, "min", &blockStat->min_value.i, pos);
        eatIntAttr(buf, "max", &blockStat->max_value.i, pos);
    }
    else if (type == "str")
    {
        eatStrAttr(buf, "min", &blockStat->min_value_str, pos);
        eatStrAttr(buf, "max", &blockStat->max_value_str, pos);
        eatIntAttr(buf, "min_len", &blockStat->min_len, pos);
        eatIntAttr(buf, "max_len", &blockStat->max_len, pos);
    }
}

void eatSingleColumn(char *buf, int &pos, db721_Column *column)
{
    int index = 0;
    eatStrAttr(buf, "type", &column->type, pos);
    eatString(buf, "\"block_stats\": {", pos);
    while (buf[pos] != '}')
    {
        eatString(buf, "\"" + std::to_string(index) + "\": {", pos);
        BlockStats blockStat;
        eatIntAttr(buf, "num", &blockStat.num, pos);
        eatBlockStat(buf, pos, column->type, &blockStat);
        column->block_stats.push_back(blockStat);
        eatString(buf, "}", pos);
        eatWhiteSpace(buf, pos);
        eatComma(buf, pos);
    }
    eatString(buf, "}", pos);
    eatComma(buf, pos);
    eatIntAttr(buf, "num_blocks", &column->num_blocks, pos);
    eatIntAttr(buf, "start_offset", &column->start_offset, pos);
}

void eatColumns(char *buf, int &pos, std::vector<db721_Column> *columns)
{
    std::string tmp;
    eatString(buf, "\"Columns\": {", pos);
    while (buf[pos] != '}')
    {
        db721_Column *column = new db721_Column();
        eatString(buf, "\"", pos);
        getString(buf, &column->name, pos);
        eatString(buf, "\": {", pos);
        eatSingleColumn(buf, pos, column);
        eatString(buf, "}", pos);
        eatWhiteSpace(buf, pos);
        eatComma(buf, pos);
        columns->push_back(*column);
    }
    eatString(buf, "}", pos);
    eatComma(buf, pos);
}

inline void eatMaxValuePerBlock(char *buf, int &pos, MetaData *metaData)
{
    eatIntAttr(buf, "Max Values Per Block", &metaData->max_value_per_block, pos);
}

// parse metaData in buffer which is in json format
int parseMetaData(char *buf, MetaData *metaData)
{
    // parse json
    int pos = 0;
    eatString(buf, "{", pos);
    eatTable(buf, pos, metaData);
    eatColumns(buf, pos, &metaData->columns);
    eatMaxValuePerBlock(buf, pos, metaData);
#ifdef db721_DEBUG
    std::cout << "table: " << metaData->table << std::endl;
    for (size_t i = 0; i < metaData->columns.size(); i++)
    {
        std::cout << "column: " << metaData->columns[i].name << std::endl;
        std::cout << "type: " << metaData->columns[i].type << std::endl;
        std::cout << "start_offset: " << metaData->columns[i].start_offset << std::endl;
        std::cout << "num_blocks: " << metaData->columns[i].num_blocks << std::endl;
        for (size_t j = 0; j < metaData->columns[i].block_stats.size(); j++)
        {
            std::cout << "num: " << metaData->columns[i].block_stats[j].num << std::endl;
            if (metaData->columns[i].type == "float")
            {
                std::cout << "max_value: " << metaData->columns[i].block_stats[j].max_value.f << std::endl;
                std::cout << "min_value: " << metaData->columns[i].block_stats[j].min_value.f << std::endl;
            }
            else if (metaData->columns[i].type == "int")
            {
                std::cout << "max_value: " << metaData->columns[i].block_stats[j].max_value.i << std::endl;
                std::cout << "min_value: " << metaData->columns[i].block_stats[j].min_value.i << std::endl;
            }
            else if (metaData->columns[i].type == "str")
            {
                std::cout << "max_value: " << metaData->columns[i].block_stats[j].max_value_str << std::endl;
                std::cout << "min_value: " << metaData->columns[i].block_stats[j].min_value_str << std::endl;
                std::cout << "max_len: " << metaData->columns[i].block_stats[j].max_len << std::endl;
                std::cout << "min_len: " << metaData->columns[i].block_stats[j].min_len << std::endl;
            }
        }
    }
    std::cout << "max_value_per_block: " << metaData->max_value_per_block << std::endl;
#endif
    return 0;
}

void parseFile(char *fileName, MetaData *metaData)
{
    std::fstream file;
    file.open(fileName);
    int fileLen = file.seekg(0, std::ios::end).tellg();
    file.seekg(fileLen - 4, std::ios::beg);
    int metaLen;
    file.read((char *)&metaLen, 4);
    file.seekg(fileLen - 4 - metaLen, std::ios::beg);
    char *buf = new char[metaLen];
    file.read(buf, metaLen);
    parseMetaData(buf, metaData);
    file.close();
}

int main(int argc, char **argv)
{
    MetaData metaData;
    parseFile(argv[1], &metaData);
    return 0;
}