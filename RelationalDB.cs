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
    public class RelationalDB : SmartContract
    {
        const int LengthOfLength = 8;

        const byte UINT160 = 0x10;
        const byte UINT256 = 0x11;
        const byte BOOLEAN = 0x20;
        const byte INT_VAR_LEN = 0x21;
        const byte BYTESTRING_VAR_LEN = 0x28;
        const byte INT_FIXED_LEN = 0x31;
        const byte BYTESTRING_FIXED_LEN = 0x38;
        //const byte TYPE_ARRAY = 0x40;
        //const byte TYPE_STRUCT = 0x41;
        //const byte TYPE_MAP = 0x48;
        // 'A' -> 0x41

        const byte USER_TABLE_NAME_TO_COLUMNS_PREFIX = (byte)'t';  // 0x74     user + tableName -> columnTypes
        const byte DROPPED_TABLE_NAME_TO_COLUMNS_PREFIX = (byte)'d';  // 0x64  user + tableName -> columnTypes
        const string SEPARATOR = "\xb6";  // ¶
        const byte ROWS_PREFIX = (byte)'r';  // 0x72  user + tableName + SEPARATOR + rowId -> data[]
        const byte TABLE_ROW_ID_PREFIX = (byte)'i';  // 0x69  user + tableName -> rowId: int

        [Safe]
        public static Iterator ListTables(UInt160 user, ByteString tableNamePrefix) => new StorageMap(USER_TABLE_NAME_TO_COLUMNS_PREFIX).Find(user + tableNamePrefix);
        [Safe]
        public static Iterator ListAllTables() => new StorageMap(USER_TABLE_NAME_TO_COLUMNS_PREFIX).Find();
        [Safe]
        public static Iterator ListDroppedTables(UInt160 user, ByteString tableNamePrefix) => new StorageMap(DROPPED_TABLE_NAME_TO_COLUMNS_PREFIX).Find(user + tableNamePrefix);
        [Safe]
        public static Iterator ListAllDroppedTables() => new StorageMap(DROPPED_TABLE_NAME_TO_COLUMNS_PREFIX).Find();

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
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        /// <exception cref="ArgumentException"></exception>
        public static void CreateTable(UInt160 user, ByteString tableName, ByteString columnTypes, bool useAutoIncPriKey)
        {
            ExecutionEngine.Assert(Runtime.CheckWitness(user), "witness CreateTable");
            ExecutionEngine.Assert(StdLib.MemorySearch(tableName, SEPARATOR) == -1, "SEPARATOR in tableName");
            if (columnTypes.Length > 256) throw new ArgumentOutOfRangeException("Too many columns");
            if (columnTypes.Length == 0) throw new ArgumentException("No column specified");
            int l = columnTypes.Length;
            for (int i = 0; i < l; ++i)
            {
                byte type = columnTypes[i];
                if (type == BOOLEAN || type == INT_VAR_LEN || type == BYTESTRING_VAR_LEN || 
                    type == UINT160 || type == UINT256)
                    continue;
                if (type == INT_FIXED_LEN || type == BYTESTRING_FIXED_LEN)
                {
                    ++i;
                    // now columnTypes[i] refers to the (fixed) length of the value, in count of bytes
                    if (columnTypes[i] == 0x00)
                        throw new ArgumentException("Invalid length 0x00");
                    continue;
                }
                throw new ArgumentException("Invalid type " + type);
            }

            StorageContext context = Storage.CurrentContext;
            ByteString key = user + tableName;
            ByteString droppedTable = new StorageMap(context, DROPPED_TABLE_NAME_TO_COLUMNS_PREFIX).Get(key);
            if (droppedTable != null)
                throw new ArgumentException("Table already dropped");
            StorageMap createdTable = new StorageMap(context, USER_TABLE_NAME_TO_COLUMNS_PREFIX);
            if (createdTable.Get(key) != null)
                throw new ArgumentException("Table already created");
            createdTable.Put(key, columnTypes);
            if (useAutoIncPriKey)
                new StorageMap(context, TABLE_ROW_ID_PREFIX).Put(key, 1);
            //else
            //    new StorageMap(context, TABLE_ROW_ID_PREFIX).Put(key, 0);
        }

        public static void DropTable(UInt160 user, ByteString tableName)
        {
            ExecutionEngine.Assert(Runtime.CheckWitness(user), "witness DropTable");
            StorageContext context = Storage.CurrentContext;
            ByteString key = user + tableName;
            StorageMap tableToColumns = new StorageMap(context, USER_TABLE_NAME_TO_COLUMNS_PREFIX);
            ByteString columnTypes = tableToColumns.Get(key);
            if (columnTypes == null || columnTypes.Length == 0)
                throw new ArgumentException("No table at id " + tableName);
            tableToColumns.Delete(key);
            new StorageMap(context, DROPPED_TABLE_NAME_TO_COLUMNS_PREFIX).Put(key, columnTypes);
        }

        public static ByteString EncodeSingle(object data, byte type, BigInteger length)
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="user"></param>
        /// <param name="tableName"></param>
        /// <param name="row"></param>
        /// <exception cref="ArgumentException"></exception>
        public static void WriteRow(UInt160 user, ByteString tableName, object[] row)
        {
            ExecutionEngine.Assert(Runtime.CheckWitness(user), "witness WriteRow");
            ExecutionEngine.Assert(row.Length > 0, "No row");
            StorageContext context = Storage.CurrentContext;
            ByteString key = user + tableName;

            ByteString columnTypes = new StorageMap(context, USER_TABLE_NAME_TO_COLUMNS_PREFIX)[key];
            BigInteger columnTypesLength = columnTypes.Length;
            BigInteger rowLength = row.Length;
            int columnTypesIndexer = 0;
            int rowIndexer = 0;

            // whether this table use auto-increment primary key
            StorageMap tableRowId = new StorageMap(context, TABLE_ROW_ID_PREFIX);
            ByteString rowId = tableRowId[key];
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

            ByteString writeContent = "";
            while (columnTypesIndexer < columnTypesLength)
            {
                ExecutionEngine.Assert(rowIndexer < rowLength, "Wrong count of columns: rowIndexer=" + rowIndexer);
                byte type = columnTypes[columnTypesIndexer++];
                if (type == INT_FIXED_LEN || type == BYTESTRING_FIXED_LEN)
                    writeContent += EncodeSingle(row[rowIndexer++], type, columnTypes[columnTypesIndexer++]);
                else
                    writeContent += EncodeSingle(row[rowIndexer++], type, 0);
            }
            new StorageMap(context, ROWS_PREFIX).Put(key + SEPARATOR + primaryKey, writeContent);
            if (rowId != null)
                tableRowId.Put(key, (BigInteger)rowId + 1);
        }

        public static void DeleteRow(UInt160 user, ByteString tableName, ByteString primaryKey)
        {
            ExecutionEngine.Assert(Runtime.CheckWitness(user), "witness DeleteRow");
            new StorageMap(ROWS_PREFIX).Delete(user + tableName + SEPARATOR + primaryKey);
        }

        public static (object, byte[]) DecodeSingle(byte[] encoded, byte type, byte length)
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="user"></param>
        /// <param name="tableName"></param>
        /// <param name="primaryKey">rowId or content of 1st column</param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        public static object[] GetRow(UInt160 user, ByteString tableName, object primaryKey)
        {
            StorageContext context = Storage.CurrentContext;
            ByteString key = user + tableName;
            ByteString columnTypes = new StorageMap(context, USER_TABLE_NAME_TO_COLUMNS_PREFIX)[key];
            if (columnTypes == null)
                throw new ArgumentException("No table");
            ByteString rowId = new StorageMap(context, TABLE_ROW_ID_PREFIX)[key];
            BigInteger columnTypesLength = columnTypes.Length;
            byte[] data = (byte[])new StorageMap(context, ROWS_PREFIX)[key + SEPARATOR + (ByteString)primaryKey];
            if (data == null)
                throw new ArgumentException("No data");
            List<object> row = new();
            int columnTypesIndexer = 0;
            if (rowId == null)
            {
                // skip the first column of columnTypes, because it is the primary key
                byte primaryKeyType = columnTypes[columnTypesIndexer++];
                if (primaryKeyType == INT_FIXED_LEN || primaryKeyType == BYTESTRING_FIXED_LEN)
                    columnTypesIndexer++;
                row.Add(primaryKey);
            }
            while (columnTypesIndexer < columnTypesLength)
            {
                byte type = columnTypes[columnTypesIndexer++];
                switch (type)
                {
                    case BOOLEAN:
                        row.Add(data[0] > 0 ? true : false);
                        data = data[1..];
                        break;
                    case UINT160:
                        row.Add(data[..20]);
                        data = data[20..];
                        break;
                    case UINT256:
                        row.Add(data[..32]);
                        data = data[32..];
                        break;
                    case INT_VAR_LEN:
                        (BigInteger i, data) = DecodeInteger(data);
                        row.Add(i);
                        break;
                    case BYTESTRING_VAR_LEN:
                        (ByteString s, data) = DecodeByteString(data);
                        row.Add(s);
                        break;
                    case INT_FIXED_LEN:
                        (BigInteger iFixed, data) = DecodeIntegerFixedLength(data, columnTypes[columnTypesIndexer++]);
                        row.Add(iFixed);
                        break;
                    case BYTESTRING_FIXED_LEN:
                        (ByteString sFixed, data) = DecodeByteStringFixedLength(data, columnTypes[columnTypesIndexer++]);
                        row.Add(sFixed);
                        break;
                    default:
                        throw new ArgumentException("Unsupported type " +  type);
                }
            }
            return row;
        }

        public static ByteString EncodeInteger(BigInteger i) => EncodeByteString((ByteString)i);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns>length(64bits; 8 bytes) + value</returns>
        /// <exception cref="ArgumentException"></exception>
        public static ByteString EncodeByteString(ByteString value)
        {
            ByteString length = (ByteString)(BigInteger)value.Length;
            BigInteger lengthToComplement = LengthOfLength - length.Length;
            //if (lengthToComplement == 0)  // not likely to happen
            //    return length + value;
            if (lengthToComplement < 0) throw new ArgumentOutOfRangeException("Integer too large");
            ByteString suffix = "";
            for (; lengthToComplement > 0; --lengthToComplement)
                suffix += new byte[] { 0x00 };
            return length + suffix + value;
            // length is little-endian. \x00 should be appended after it.
        }

        public static (ByteString, byte[]) DecodeByteString(byte[] encoded)
        {
            int length = (int)(BigInteger)(ByteString)encoded[0..LengthOfLength];
            int contentEndingIndex = LengthOfLength + length;
            return ((ByteString)encoded[LengthOfLength..contentEndingIndex], encoded[contentEndingIndex..]);
        }

        public static (BigInteger, byte[]) DecodeInteger(byte[] encoded)
        {
            (ByteString, byte[]) result = DecodeByteString(encoded);
            return ((BigInteger)result.Item1, result.Item2);
        }

        public static ByteString EncodeIntegerFixedLength(BigInteger i, BigInteger fixedLength)
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
        public static ByteString EncodeByteStringFixedLength(ByteString data, BigInteger fixedLength)
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
        public static (ByteString, byte[]) DecodeByteStringFixedLength(byte[] encoded, int fixedLength)
        {
            ByteString d = (ByteString)encoded[..fixedLength];
            encoded = encoded[fixedLength..];
            return (d, encoded);
        }
        public static (BigInteger, byte[]) DecodeIntegerFixedLength(byte[] encoded, int fixedLength)
        {
            (ByteString d, encoded) = DecodeByteStringFixedLength(encoded, fixedLength);
            return ((BigInteger)d, encoded);
        }
    }
}
