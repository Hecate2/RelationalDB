using System;
using System.ComponentModel;
using System.Numerics;
using Neo;
using Neo.SmartContract.Framework;
using Neo.SmartContract.Framework.Attributes;
using Neo.SmartContract.Framework.Native;
using Neo.SmartContract.Framework.Services;

namespace RelationalDB
{
    [DisplayName("RelationalDB")]
    [ManifestExtra("Author", "Hecate2")]
    [ManifestExtra("Email", "developer@neo.org")]
    [ManifestExtra("Description", "This is a RelationalDB")]
    public partial class RelationalDB : SmartContract
    {
        const bool USE_INDEX_FOR_INTEGER = true;

        //const int LengthOfLength = 2;  // Neo3 allows only value <= 65535 == 2**16-1 bytes

        const byte UINT160 = 0x10;
        const byte UINT256 = 0x11;
        const byte BOOLEAN = 0x20;
        // INT_VAR_LEN costs additional 2 bytes in storage, for length of int
        const byte INT_VAR_LEN = 0x21;   // Neo3 only allows BigInteger <= 32 bytes to be decoded
        const byte BYTESTRING_VAR_LEN = 0x28;
        const byte INT_FIXED_LEN = 0x31;
        const byte BYTESTRING_FIXED_LEN = 0x38;
        //const byte TYPE_ARRAY = 0x40;
        //const byte TYPE_STRUCT = 0x41;
        //const byte TYPE_MAP = 0x48;
        // 'A' -> 0x41

        const byte USER_TABLE_NAME_TO_COLUMNS_PREFIX = (byte)'t';  // 0x74     user + tableName -> columnTypes
        const byte DROPPED_TABLE_NAME_TO_COLUMNS_PREFIX = (byte)'d';  // 0x64  user + tableName -> columnTypes
        const byte COLUMN_NAME_PREFIX = (byte)'n';  // 0x6e                    user + tableName + SEPARATOR + columnName -> columnId
        const byte COLUMN_ID_PREFIX = (byte)'c';  // 0x63                      user + tableName + SEPARATOR + columnId -> columnType
        // Column ID always starts from 1 for the 1st column of your data
        const string SEPARATOR = "\x00";
        const byte ROWS_PREFIX = (byte)'r';  // 0x72  user + tableName + SEPARATOR + rowId -> data[]
        const byte TABLE_ROW_ID_PREFIX = (byte)'i';  // 0x69  user + tableName -> rowId: int

        [Safe]
        public ByteString GetColumnTypes(UInt160 user, ByteString tableName) => new StorageMap(USER_TABLE_NAME_TO_COLUMNS_PREFIX).Get(user + tableName);
        [Safe]
        public ByteString GetColumnType(UInt160 user, ByteString tableName, byte columnId) => new StorageMap(COLUMN_ID_PREFIX).Get(user + tableName + SEPARATOR + (ByteString)new byte[] { columnId });
        [Safe]
        public Iterator FindColumnNames(UInt160 user, ByteString tableName, ByteString columnNamePrefix) => new StorageMap(COLUMN_NAME_PREFIX).Find(user + tableName + SEPARATOR + columnNamePrefix);
        [Safe]
        public ByteString GetColumnTypeByName(UInt160 user, ByteString tableName, ByteString columnName)
        {
            StorageContext context = Storage.CurrentContext;
            ByteString tableKey = user + tableName + SEPARATOR;
            return new StorageMap(context, COLUMN_ID_PREFIX)[tableKey +
                new StorageMap(context, COLUMN_NAME_PREFIX)[tableKey + columnName]
            ];
        }
        [Safe]
        public ByteString GetColumnTypesDropped(UInt160 user, ByteString tableName) => new StorageMap(DROPPED_TABLE_NAME_TO_COLUMNS_PREFIX).Get(user + tableName);
        [Safe]
        public BigInteger GetRowId(UInt160 user, ByteString tableName) => (BigInteger)new StorageMap(TABLE_ROW_ID_PREFIX).Get(user + tableName);
        [Safe]
        public Iterator ListTables(UInt160 user, ByteString tableNamePrefix) => new StorageMap(USER_TABLE_NAME_TO_COLUMNS_PREFIX).Find(user + tableNamePrefix);
        [Safe]
        public Iterator ListAllTables() => new StorageMap(USER_TABLE_NAME_TO_COLUMNS_PREFIX).Find();
        [Safe]
        public Iterator ListDroppedTables(UInt160 user, ByteString tableNamePrefix) => new StorageMap(DROPPED_TABLE_NAME_TO_COLUMNS_PREFIX).Find(user + tableNamePrefix);
        [Safe]
        public Iterator ListAllDroppedTables() => new StorageMap(DROPPED_TABLE_NAME_TO_COLUMNS_PREFIX).Find();

        /// <summary>
        /// Be aware that Neo3 allows only storage key no more than 64 bytes
        /// <see cref="user"/> costs 20 bytes; a separator costs 1 byte; a prefix costs 1 byte
        /// you have only the remaining 42 bytes for table name and primary key
        /// DO NOT USE VERY LONG TABLE NAME
        /// </summary>
        /// <param name="user"></param>
        /// <param name="columnTypes"></param>
        /// <param name="tableName"></param>
        /// <param name="useAutoIncPriKey">
        /// if true, an incremental rowId (starting from 1) is automatically generated for each row
        /// if false, first column is used as primary key. No duplicating primary key allowed
        /// </param>
        /// <returns>count of columns</returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        /// <exception cref="ArgumentException"></exception>
        public BigInteger CreateTable(UInt160 user, ByteString tableName, ByteString columnTypes, bool useAutoIncPriKey)
        {
            ExecutionEngine.Assert(Runtime.CheckWitness(user), "witness CreateTable");
            ExecutionEngine.Assert(StdLib.MemorySearch(tableName, SEPARATOR) == -1, "SEPARATOR in tableName");
            if (columnTypes.Length >= 256) throw new ArgumentOutOfRangeException("Too many columns");
            if (columnTypes.Length == 0) throw new ArgumentException("No column specified");

            StorageContext context = Storage.CurrentContext;
            ByteString tableKey = user + tableName;
            ByteString droppedTable = new StorageMap(context, DROPPED_TABLE_NAME_TO_COLUMNS_PREFIX).Get(tableKey);
            if (droppedTable != null)
                throw new ArgumentException("Table already dropped");
            StorageMap createdTable = new StorageMap(context, USER_TABLE_NAME_TO_COLUMNS_PREFIX);
            if (createdTable.Get(tableKey) != null)
                throw new ArgumentException("Table already created");

            int l = columnTypes.Length;
            byte columnId = 0;
            for (int i = 0; i < l; ++i)
            {
                StorageMap columnTypeMap = new(context, COLUMN_ID_PREFIX);
                byte type = columnTypes[i];
                if (type == BOOLEAN || type == INT_VAR_LEN || type == BYTESTRING_VAR_LEN || 
                    type == UINT160 || type == UINT256)
                {
                    columnTypeMap.Put(tableKey + SEPARATOR + (ByteString)new byte[] { ++columnId }, type);
                    continue;
                }
                if (type == INT_FIXED_LEN || type == BYTESTRING_FIXED_LEN)
                {
                    columnTypeMap.Put(tableKey + SEPARATOR + (ByteString)new byte[] { ++columnId }, (ByteString)new byte[] { type, columnTypes[++i] });
                    // now columnTypes[i] refers to the (fixed) length of the value, in count of bytes
                    ExecutionEngine.Assert(columnTypes[i] != 0x00, "Invalid length 0x00");
                    continue;
                }
                ExecutionEngine.Assert(false, "Invalid type");
            }

            createdTable.Put(tableKey, columnTypes);
            if (useAutoIncPriKey)
                new StorageMap(context, TABLE_ROW_ID_PREFIX).Put(tableKey, 1);
            //else
            //    new StorageMap(context, TABLE_ROW_ID_PREFIX).Put(key, 0);
            return columnId;
        }

        /// <summary>
        /// It's the user's responsibility to use a unique name for each column. We do not check it.
        /// </summary>
        /// <param name="user"></param>
        /// <param name="tableName"></param>
        /// <param name="columnNames"></param>
        public void SetColumnNames(UInt160 user, ByteString tableName, ByteString[] columnNames)
        {
            ExecutionEngine.Assert(Runtime.CheckWitness(user), "witness SetColumnName");
            StorageContext context = Storage.CurrentContext;
            StorageMap columnNameMap = new(context, COLUMN_NAME_PREFIX);
            StorageMap columnTypeMap = new(context, COLUMN_ID_PREFIX);
            byte i = 1;
            BigInteger namesLength = columnNames.Length;
            ByteString tableKey = user + tableName + SEPARATOR;
            while (i <= namesLength)
            {
                ExecutionEngine.Assert(columnTypeMap[tableKey + (ByteString)(BigInteger)i] != null, "No column");
                columnNameMap.Put(tableKey + columnNames[i-1], i);
                i++;
            }
        }

        public void DropTable(UInt160 user, ByteString tableName)
        {
            ExecutionEngine.Assert(Runtime.CheckWitness(user), "witness DropTable");
            StorageContext context = Storage.CurrentContext;
            ByteString tableKey = user + tableName;
            StorageMap tableToColumns = new StorageMap(context, USER_TABLE_NAME_TO_COLUMNS_PREFIX);
            ByteString columnTypes = tableToColumns.Get(tableKey);
            if (columnTypes == null || columnTypes.Length == 0)
                throw new ArgumentException("No table at id " + tableName);
            tableToColumns.Delete(tableKey);
            new StorageMap(context, DROPPED_TABLE_NAME_TO_COLUMNS_PREFIX).Put(tableKey, columnTypes);
        }

        [Safe]
        public ByteString EncodeSingle(object data, byte type, BigInteger length)
        {
            switch (type)
            {
                case BOOLEAN:
                    return (bool)data ? "\x01" : "\x00";
                case INT_VAR_LEN:
                case BYTESTRING_VAR_LEN:
                    return EncodeByteString((ByteString)data);
                case UINT160:
                    return EncodeByteStringFixedLength((UInt160)data, 20);
                case UINT256:
                    return EncodeByteStringFixedLength((UInt256)data, 32);
                case INT_FIXED_LEN:
                    return EncodeIntegerFixedLength((BigInteger)data, length);
                case BYTESTRING_FIXED_LEN:
                    return EncodeByteStringFixedLength((ByteString)data, length);
                default:
                    throw new ArgumentException("Unsupported type " + type);
            }
        }

        [Safe]
        public ByteString EncodeRow(object[] row, byte[] columnTypes)
        {
            int rowIndexer = 0, columnTypesIndexer = 0;
            BigInteger rowLength = row.Length, columnTypesLength = columnTypes.Length;
            ByteString writeContent = "";
            while (columnTypesIndexer < columnTypesLength)
            {
                ExecutionEngine.Assert(rowIndexer < rowLength, "Wrong column count");
                byte type = columnTypes[columnTypesIndexer++];
                if (type == INT_FIXED_LEN || type == BYTESTRING_FIXED_LEN)
                    writeContent += EncodeSingle(row[rowIndexer++], type, columnTypes[columnTypesIndexer++]);
                else
                    writeContent += EncodeSingle(row[rowIndexer++], type, 0);
            }
            return writeContent;
        }

        protected void WriteIndex(ByteString tableKey, object[] row, byte[] columnTypes)
        {
            int rowLength = row.Length;
            byte rowIndexer = 0, columnTypesIndexer = 0;
            tableKey += SEPARATOR;
            while (rowIndexer < rowLength)
            {
                byte type = columnTypes[columnTypesIndexer++];
                ++rowIndexer;
                if (type == INT_FIXED_LEN || type == INT_VAR_LEN)
                    SplayInsert(tableKey + (ByteString)new byte[] { rowIndexer }, (ByteString)row[rowIndexer-1]);
                if (type == INT_FIXED_LEN || type == BYTESTRING_FIXED_LEN)
                    columnTypesIndexer++;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="user"></param>
        /// <param name="tableName"></param>
        /// <param name="row"></param>
        /// <exception cref="ArgumentException"></exception>
        public void WriteRow(UInt160 user, ByteString tableName, object[] row)
        {
            ExecutionEngine.Assert(Runtime.CheckWitness(user), "witness WriteRow");
            BigInteger rowLength = row.Length;
            ExecutionEngine.Assert(rowLength > 0 && rowLength < 256, "No row");
            StorageContext context = Storage.CurrentContext;
            ByteString tableKey = user + tableName;

            byte[] columnTypes = (byte[])new StorageMap(context, USER_TABLE_NAME_TO_COLUMNS_PREFIX)[tableKey];
            int columnTypesIndexer = 0;
            int rowIndexer = 0;

            // whether this table use auto-increment primary key
            StorageMap tableRowId = new StorageMap(context, TABLE_ROW_ID_PREFIX);
            ByteString rowId = tableRowId[tableKey];
            ByteString primaryKey;
            if (rowId == null)  // use 1st column of your data as primary key
            {
                byte type = columnTypes[columnTypesIndexer++];
                if (type == INT_FIXED_LEN || type == BYTESTRING_FIXED_LEN)
                    primaryKey = EncodeSingle(row[rowIndexer++], type, columnTypes[columnTypesIndexer++]);
                else
                    primaryKey = EncodeSingle(row[rowIndexer++], type, 0);
            }
            else
                primaryKey = rowId;
            if (USE_INDEX_FOR_INTEGER)
            {
                DeleteRow(user, tableName, primaryKey);
                WriteIndex(tableKey, row, columnTypes);
            }

            // NC2010: The type object[] does not support range access.
            // Cannot write row[rowIndexer..]
            List<object> rowWithoutPrimaryKey = new();
            while(rowIndexer < rowLength)
                rowWithoutPrimaryKey.Add(row[rowIndexer++]);
            new StorageMap(context, ROWS_PREFIX).Put(tableKey + SEPARATOR + primaryKey,
                EncodeRow(rowWithoutPrimaryKey, columnTypes[columnTypesIndexer..]));
            if (rowId != null)
                tableRowId.Put(tableKey, (BigInteger)rowId + 1);
        }

        /// <summary>
        /// Re-entrancy risk!
        /// </summary>
        /// <param name="user"></param>
        /// <param name="tableName"></param>
        /// <param name="rows"></param>
        public void WriteRows(UInt160 user, ByteString tableName, object[][] rows)
        {
            ExecutionEngine.Assert(Runtime.CheckWitness(user), "witness WriteRow");
            ExecutionEngine.Assert(rows.Length > 0, "No rows");
            StorageContext context = Storage.CurrentContext;
            ByteString tableKey = user + tableName;
            byte[] columnTypes = (byte[])new StorageMap(context, USER_TABLE_NAME_TO_COLUMNS_PREFIX)[tableKey];
            BigInteger rowLength = rows[0].Length;
            ExecutionEngine.Assert(rowLength > 0 && rowLength < 256, "No row");
            // whether this table use auto-increment primary key
            StorageMap tableRowId = new StorageMap(context, TABLE_ROW_ID_PREFIX);
            ByteString rowId = tableRowId[tableKey];

            foreach (object[] row in rows)
            {
                ExecutionEngine.Assert(row.Length == rowLength, "Inconsistent row length");
                int columnTypesIndexer = 0;
                int rowIndexer = 0;

                ByteString primaryKey;
                if (rowId == null)
                {
                    byte type = columnTypes[columnTypesIndexer++];
                    if (type == INT_FIXED_LEN || type == BYTESTRING_FIXED_LEN)
                        primaryKey = EncodeSingle(row[rowIndexer++], type, columnTypes[columnTypesIndexer++]);
                    else
                        primaryKey = EncodeSingle(row[rowIndexer++], type, 0);
                }
                else
                    primaryKey = rowId;
                if (USE_INDEX_FOR_INTEGER)
                {
                    DeleteRow(user, tableName, primaryKey);
                    WriteIndex(tableKey, row, columnTypes);
                }

                // NC2010: The type object[] does not support range access.
                // Cannot write row[rowIndexer..]
                List<object> rowWithoutPrimaryKey = new();
                while (rowIndexer < rowLength)
                    rowWithoutPrimaryKey.Add(row[rowIndexer++]);
                new StorageMap(context, ROWS_PREFIX).Put(tableKey + SEPARATOR + primaryKey,
                    EncodeRow(rowWithoutPrimaryKey, columnTypes[columnTypesIndexer..]));
                if (rowId != null)
                    tableRowId.Put(tableKey, (BigInteger)rowId + 1);
            }
        }

        protected void DeleteIndex(ByteString tableKey, object[] row, byte[] columnTypes)
        {
            int rowLength = row.Length;
            byte rowIndexer = 0, columnTypesIndexer = 0;
            tableKey += SEPARATOR;
            while (rowIndexer < rowLength)
            {
                byte type = columnTypes[columnTypesIndexer++];
                rowIndexer++;
                if (type == INT_FIXED_LEN || type == INT_VAR_LEN)
                    SplayDelete(tableKey + (ByteString)new byte[] { rowIndexer }, (ByteString)row[rowIndexer-1]);
                if (type == INT_FIXED_LEN || type == BYTESTRING_FIXED_LEN)
                    columnTypesIndexer++;
            }
        }

        public void DeleteRow(UInt160 user, ByteString tableName, ByteString primaryKey)
        {
            ExecutionEngine.Assert(Runtime.CheckWitness(user), "witness DeleteRow");
            try
            {
                ByteString tableKey = user + tableName;
                object[] row = GetRow(tableKey, primaryKey);
                DeleteIndex(tableKey, row, (byte[])new StorageMap(USER_TABLE_NAME_TO_COLUMNS_PREFIX)[tableKey]);
                new StorageMap(ROWS_PREFIX).Delete(tableKey + SEPARATOR + primaryKey);
            }catch (Exception)
            {
                return;
            }
        }
        /// <summary>
        /// Re-entrancy risk!
        /// </summary>
        /// <param name="user"></param>
        /// <param name="tableName"></param>
        /// <param name="primaryKeys"></param>
        public void DeleteRows(UInt160 user, ByteString tableName, ByteString[] primaryKeys)
        {
            ExecutionEngine.Assert(Runtime.CheckWitness(user), "witness DeleteRow");
            foreach (ByteString primaryKey in primaryKeys)
                try
                {
                    ByteString tableKey = user + tableName;
                    object[] row = GetRow(tableKey, primaryKey);
                    DeleteIndex(tableKey, row, (byte[])new StorageMap(USER_TABLE_NAME_TO_COLUMNS_PREFIX)[tableKey]);
                    new StorageMap(ROWS_PREFIX).Delete(tableKey + SEPARATOR + primaryKey);
                }
                catch (Exception)
                {
                    continue;
                }
        }

        [Safe]
        public (object, byte[]) DecodeSingle(byte[] encoded, byte type, byte length)
        {
            switch (type)
            {
                case BOOLEAN:
                    return (encoded[0] > 0 ? true : false, encoded[1..]);
                case UINT160:
                    return (encoded[..20], encoded[20..]);
                case UINT256:
                    return (encoded[..32], encoded[32..]);
                case INT_VAR_LEN:
                    return DecodeInteger(encoded);
                case BYTESTRING_VAR_LEN:
                    return DecodeByteString(encoded);
                case INT_FIXED_LEN:
                    return DecodeIntegerFixedLength(encoded, length);
                case BYTESTRING_FIXED_LEN:
                    return DecodeByteStringFixedLength(encoded, length);
                default:
                    throw new ArgumentException("Unsupported type " + type);
            }
        }

        [Safe]
        public List<object> DecodeRow(byte[] data, byte[] columnTypes) => DecodeRow(data, columnTypes, new List<object>());
        [Safe]
        public List<object> DecodeRow(byte[] data, byte[] columnTypes, List<object> row)
        {
            BigInteger columnTypesLength = columnTypes.Length;
            int columnTypesIndexer = 0;
            while (columnTypesIndexer < columnTypesLength)
            {
                byte type = columnTypes[columnTypesIndexer++];
                if (type == INT_FIXED_LEN || type == BYTESTRING_FIXED_LEN)
                {
                    (object decoded, data) = DecodeSingle(data, type, columnTypes[columnTypesIndexer++]);
                    row.Add(decoded);
                }
                else
                {
                    (object decoded, data) = DecodeSingle(data, type, 0);
                    row.Add(decoded);
                }
            }
            return row;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="user"></param>
        /// <param name="tableName"></param>
        /// <param name="primaryKey">rowId or content of 1st column</param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        public object[] GetRow(UInt160 user, ByteString tableName, ByteString primaryKey) => GetRow(user + tableName, primaryKey);
        public object[] GetRow(ByteString tableKey, ByteString primaryKey)
        {
            StorageContext context = Storage.CurrentContext;
            byte[] columnTypes = (byte[])new StorageMap(context, USER_TABLE_NAME_TO_COLUMNS_PREFIX)[tableKey];
            if (columnTypes == null)
                throw new ArgumentException("No table");
            ByteString rowId = new StorageMap(context, TABLE_ROW_ID_PREFIX)[tableKey];
            byte[] data = (byte[])new StorageMap(context, ROWS_PREFIX)[tableKey + SEPARATOR + primaryKey];
            if (data == null)
                throw new ArgumentException("No data");
            List<object> row = new();
            int columnTypesIndexer = 0;
            if (rowId == null)
            {
                // skip the first column of columnTypes, because it is the primary key
                byte primaryKeyType = columnTypes[columnTypesIndexer++];
                object result;
                if (primaryKeyType == INT_FIXED_LEN || primaryKeyType == BYTESTRING_FIXED_LEN)
                    (result, _) = DecodeSingle((byte[])primaryKey, primaryKeyType, columnTypes[columnTypesIndexer++]);
                else
                    (result, _) = DecodeSingle((byte[])primaryKey, primaryKeyType, 0);
                row.Add(result);
            }
            return DecodeRow(data, columnTypes[columnTypesIndexer..], row);
        }

        public object[][] GetRows(UInt160 user, ByteString tableName, ByteString[] primaryKeys) => GetRows(user + tableName, primaryKeys);
        public object[][] GetRows(ByteString tableKey, ByteString[] primaryKeys)
        {
            StorageContext context = Storage.CurrentContext;
            byte[] columnTypes = (byte[])new StorageMap(context, USER_TABLE_NAME_TO_COLUMNS_PREFIX)[tableKey];
            if (columnTypes == null)
                throw new ArgumentException("No table");
            ByteString rowId = new StorageMap(context, TABLE_ROW_ID_PREFIX)[tableKey];

            List<object[]> resultRows = new();
            foreach(ByteString primaryKey in primaryKeys)
            {
                byte[] data = (byte[])new StorageMap(context, ROWS_PREFIX)[tableKey + SEPARATOR + primaryKey];
                ExecutionEngine.Assert(data != null, "No data");
                List<object> row = new();
                int columnTypesIndexer = 0;
                if (rowId == null)
                {
                    // skip the first column of columnTypes, because it is the primary key
                    byte primaryKeyType = columnTypes[columnTypesIndexer++];
                    object result;
                    if (primaryKeyType == INT_FIXED_LEN || primaryKeyType == BYTESTRING_FIXED_LEN)
                        (result, _) = DecodeSingle((byte[])primaryKey, primaryKeyType, columnTypes[columnTypesIndexer++]);
                    else
                        (result, _) = DecodeSingle((byte[])primaryKey, primaryKeyType, 0);
                    row.Add(result);
                }
                resultRows.Add(DecodeRow(data, columnTypes[columnTypesIndexer..], row));
            }
            return resultRows;
        }

        public Iterator ListRows(UInt160 user, ByteString tableName) => ListRows(user + tableName);
        public Iterator ListRows(ByteString tableKey) => new StorageMap(ROWS_PREFIX).Find(tableKey, FindOptions.RemovePrefix);

        [Safe]
        public ByteString EncodeInteger(BigInteger i) => EncodeByteString((ByteString)i);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns>value.Length(16bits/2bytes unsigned int) + value</returns>
        /// <exception cref="ArgumentException"></exception>
        [Safe]
        public ByteString EncodeByteString(ByteString value)
        {
            BigInteger lengthInt = (BigInteger)value.Length;
            ByteString length = (ByteString)lengthInt;
            switch (length.Length)
            {
                case 1:
                    return length + "\x00" + value;
                    // length is little-endian. \x00 should be appended after it.
                case 2:
                    return length + value;
                case 3:
                    if (lengthInt <= 65535)
                        return (ByteString)new byte[] { length[0], length[1] } + value;
                    goto default;
                default:
                    throw new ArgumentOutOfRangeException("Too long " + length.Length);
            }
        }

        [Safe]
        public (ByteString, byte[]) DecodeByteString(byte[] encoded)
        {
            int length = (int)(BigInteger)(ByteString)(new byte[] { encoded[0], encoded[1], 0x00 });
            return ((ByteString)encoded[2..(2+length)], encoded[(2+length)..]);
        }

        [Safe]
        public (BigInteger, byte[]) DecodeInteger(byte[] encoded)
        {
            (ByteString, byte[]) result = DecodeByteString(encoded);
            return ((BigInteger)result.Item1, result.Item2);
        }

        [Safe]
        public ByteString EncodeIntegerFixedLength(BigInteger i, BigInteger fixedLength)
        {
            ByteString data = (ByteString)i;
            BigInteger lengthToComplement = fixedLength - data.Length;
            if (lengthToComplement < 0)
                throw new ArgumentException("Too long value " + data);
            if (lengthToComplement > 0)
            {
                ByteString suffix = "";
                for (; lengthToComplement > 0; --lengthToComplement)
                    suffix += i >= 0 ? new byte[] { 0x00 } : new byte[] { 0xff };
                data += suffix;
            }
            return data;
        }

        [Safe]
        public ByteString EncodeByteStringFixedLength(ByteString data, BigInteger fixedLength)
        {
            BigInteger lengthToComplement = fixedLength - data.Length;
            if (lengthToComplement < 0)
                throw new ArgumentException("Too long value " + data);
            if (lengthToComplement > 0)
            {
                ByteString suffix = "";
                for (; lengthToComplement > 0; --lengthToComplement)
                    suffix += new byte[] { 0x00 };
                data += suffix;
            }
            return data;
        }

        [Safe]
        public (ByteString, byte[]) DecodeByteStringFixedLength(byte[] encoded, int fixedLength)
        {
            ByteString d = (ByteString)encoded[..fixedLength];
            encoded = encoded[fixedLength..];
            return (d, encoded);
        }

        [Safe]
        public (BigInteger, byte[]) DecodeIntegerFixedLength(byte[] encoded, int fixedLength)
        {
            (ByteString d, encoded) = DecodeByteStringFixedLength(encoded, fixedLength);
            return ((BigInteger)d, encoded);
        }
    }
}
