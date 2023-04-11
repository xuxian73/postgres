extern "C" {
    #include "../../../../src/include/postgres.h"
    #include "../../../../src/include/fmgr.h"
    #include "../../../../src/include/executor/tuptable.h"
}
// #include "dog.h"
#include "fstream"
#include "string.h"
#include <vector>
#include <iostream>

// #define db721_DEBUG

enum db721_Type
{
    db721_INT,
    db721_FLOAT,
    db721_STR
};

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
    char* max_value_str;
    char* min_value_str;
};

struct db721_Column
{
    char* name;
    db721_Type type;
    // the offset in the file for the first block of this column
    int32_t start_offset;
    // the number of blocks for this column
    int32_t num_blocks;
    std::vector<BlockStats> block_stats;
};

struct MetaData
{
    char* table;
    int32_t max_value_per_block;
    int32_t num_columns;
    std::vector<db721_Column> columns;
};

db721_Type db721_getType(char* type) {
    if (strcmp(type, "int") == 0)
    {
        return db721_INT;
    }
    else if (strcmp(type, "float") == 0)
    {
        return db721_FLOAT;
    }
    else if (strcmp(type, "str") == 0)
    {
        return db721_STR;
    }
    else
    {
        std::cout << "Error: Unknown type: " << type << std::endl;
        return db721_INT;
    }
}

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

inline void getString(char *buf, char *&str, int &pos)
{
    int tmp = pos;
    while (buf[pos] != '"')
    {
        pos++;
    }
    str = pnstrdup(buf + tmp, pos - tmp);
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

inline void eatStrAttr(char *buf, std::string attr, char *&str, int &pos)
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
    eatStrAttr(buf, "Table", metaData->table, pos);
}

void eatBlockStat(char *buf, int &pos, db721_Type type, BlockStats *blockStat)
{
    if (type == db721_FLOAT)
    {
        eatFloatAttr(buf, "min", &blockStat->min_value.f, pos);
        eatFloatAttr(buf, "max", &blockStat->max_value.f, pos);
    }
    else if (type == db721_INT)
    {
        eatIntAttr(buf, "min", &blockStat->min_value.i, pos);
        eatIntAttr(buf, "max", &blockStat->max_value.i, pos);
    }
    else if (type == db721_STR)
    {
        eatStrAttr(buf, "min", blockStat->min_value_str, pos);
        eatStrAttr(buf, "max", blockStat->max_value_str, pos);
        eatIntAttr(buf, "min_len", &blockStat->min_len, pos);
        eatIntAttr(buf, "max_len", &blockStat->max_len, pos);
    }
}

void eatSingleColumn(char *buf, int &pos, db721_Column *column)
{
    int index = 0;
    char * type;
    eatStrAttr(buf, "type", type, pos);
    column->type = db721_getType(type);
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
        ++index;
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
        db721_Column *column = (db721_Column*)palloc0(sizeof(db721_Column));
        eatString(buf, "\"", pos);
        getString(buf, column->name, pos);
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
    metaData->num_columns = metaData->columns.size();
    eatMaxValuePerBlock(buf, pos, metaData);
#ifdef db721_DEBUG
    for (size_t i = 0; i < metaData->columns.size(); i++)
    {
        std::cout << "column: " << metaData->columns[i].name << std::endl;
        std::cout << "type: " << metaData->columns[i].type << std::endl;
        std::cout << "start_offset: " << metaData->columns[i].start_offset << std::endl;
        std::cout << "num_blocks: " << metaData->columns[i].num_blocks << std::endl;
        for (size_t j = 0; j < metaData->columns[i].block_stats.size(); j++)
        {
            std::cout << "num: " << metaData->columns[i].block_stats[j].num << std::endl;
            if (metaData->columns[i].type == db721_FLOAT)
            {
                std::cout << "max_value: " << metaData->columns[i].block_stats[j].max_value.f << std::endl;
                std::cout << "min_value: " << metaData->columns[i].block_stats[j].min_value.f << std::endl;
            }
            else if (metaData->columns[i].type == db721_INT)
            {
                std::cout << "max_value: " << metaData->columns[i].block_stats[j].max_value.i << std::endl;
                std::cout << "min_value: " << metaData->columns[i].block_stats[j].min_value.i << std::endl;
            }
            else if (metaData->columns[i].type == db721_STR)
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

struct db721_Parser {
    MetaData metaData;
    char ** block_buf;
    int32_t current_block_id;
    int32_t current_index;
    char * fileName;

    void init(const char *fileName)
    {
        std::fstream file;
        this->fileName = pstrdup(fileName);
        file.open(fileName);
        int fileLen = file.seekg(0, std::ios::end).tellg();
        file.seekg(fileLen - 4, std::ios::beg);
        int metaLen;
        file.read((char *)&metaLen, 4);
        file.seekg(fileLen - 4 - metaLen, std::ios::beg);
        char *buf = (char*)palloc0_array(char, metaLen);
        file.read(buf, metaLen);
        parseMetaData(buf, &this->metaData);
        pfree((void*)buf);
        file.close();
        this->block_buf = (char**)palloc0_array(char *, this->metaData.num_columns);
        for (int i = 0; i < this->metaData.num_columns; i++)
        {
            int value_size = this->metaData.columns[i].type == db721_STR ? 32 : 4;
            this->block_buf[i] = (char*)palloc0_array(char, this->metaData.max_value_per_block * value_size);
        }
        readBlock(0);
    }

    void readBlock(int32_t block_id) {
        std::fstream file;
        file.open(this->fileName);
        for (int i = 0; i < this->metaData.num_columns; i++)
        {
            int value_size = this->metaData.columns[i].type == db721_STR ? 32 : 4;
            file.seekg(this->metaData.columns[i].start_offset + block_id * this->metaData.max_value_per_block * value_size, std::ios::beg);
            file.read(this->block_buf[i], this->metaData.columns[i].block_stats[block_id].num * value_size);
            // elog(INFO, "read block %d, column %d, num %d", block_id, i, this->metaData.columns[i].block_stats[block_id].num);
        }
        file.close();
    }

    bool next(TupleTableSlot *slot) {
        MemSet(slot->tts_values, 0, sizeof(Datum) * slot->tts_tupleDescriptor->natts);
        MemSet(slot->tts_isnull, false, sizeof(bool) * slot->tts_tupleDescriptor->natts);
        for (int i = 0; i < this->metaData.num_columns; i++)
        {
            char *buf = this->block_buf[i];
            if (this->metaData.columns[i].type == db721_STR)
            {
                char * buffer = buf + this->current_index * 32;
                int len = strlen(buffer);
                VarChar *destination = (VarChar *) palloc(VARHDRSZ + len);
                SET_VARSIZE(destination, VARHDRSZ + len);
                memcpy(destination->vl_dat, buffer, len);
                slot->tts_values[i] = PointerGetDatum(destination);
            }
            else if (this->metaData.columns[i].type == db721_INT)
            {
                slot->tts_values[i] = Int32GetDatum(((int32_t*)buf)[current_index]);
            }
            else if (this->metaData.columns[i].type == db721_FLOAT)
            {
                slot->tts_values[i] = Float4GetDatum(((float*)buf)[current_index]);
            }
        }
        this->current_index++;
        if (this->current_index >= this->metaData.columns[0].block_stats[this->current_block_id].num) {
            this->current_index = 0;
            this->current_block_id++;
            if (this->current_block_id >= this->metaData.columns[0].num_blocks) {
                return false;
            }
            readBlock(this->current_block_id);
        }
        return true;
    }

    void reset() {
        if (current_block_id != 0) {
            readBlock(0);
            this->current_block_id = 0;
        }
        this->current_index = 0;
    }

    void close()
    {
        for (int i = 0; i < this->metaData.num_columns; i++)
        {
            pfree(this->block_buf[i]);
        }
        pfree(this->block_buf);
    }
};