using System;
using System.ComponentModel;
using System.Numerics;
using Neo;
using Neo.SmartContract.Framework;
using Neo.SmartContract.Framework.Attributes;
using Neo.SmartContract.Framework.Native;
using Neo.SmartContract.Framework.Services;

namespace SecondaryIndex
{
    [DisplayName("RelationalDB")]
    [ManifestExtra("Author", "Hecate2")]
    [ManifestExtra("Email", "developer@neo.org")]
    [ManifestExtra("Description", "This is a SecondaryIndex")]
    public class RelationalDB : SmartContract
    {
        const int LengthOfLength = 8;

        const byte BOOLEAN = 0x20;
        const byte INT_VAR_LEN = 0x21;
        const byte BYTESTRING_VAR_LEN = 0x28;
        const byte INT_FIXED_LEN = 0x31;
        const byte BYTESTRING_FIXED_LEN = 0x38;
        //const byte TYPE_ARRAY = 0x40;
        //const byte TYPE_STRUCT = 0x41;
        //const byte TYPE_MAP = 0x48;
        // 'A' -> 0x41

        const byte USER_TABLE_NAME_TO_COLUMNS_PREFIX = (byte)'T';  // 0x54     user + tableName -> columnTypes
        const byte DROPPED_TABLE_NAME_TO_COLUMNS_PREFIX = (byte)'D';  // 0x44  user + tableName -> columnTypes
        const byte SEPARATOR = (byte)'\xb6';  // ¶
        const byte ROWS_PREFIX = (byte)'R';  // 0x52  user + tableName + SEPARATOR + rowId -> data[]
        const byte TABLE_ROW_ID_PREFIX = (byte)'I';  // 0x49  user + tableName -> rowId: int

        [Safe]
        public static Iterator ListTables(UInt160 user, ByteString tableNamePrefix) => new StorageMap(USER_TABLE_NAME_TO_COLUMNS_PREFIX).Find(user + tableNamePrefix);
        [Safe]
        public static Iterator ListAllTables() => new StorageMap(USER_TABLE_NAME_TO_COLUMNS_PREFIX).Find();
        [Safe]
        public static Iterator ListDroppedTables(UInt160 user, ByteString tableNamePrefix) => new StorageMap(DROPPED_TABLE_NAME_TO_COLUMNS_PREFIX).Find(user + tableNamePrefix);
        [Safe]
        public static Iterator ListAllDroppedTables() => new StorageMap(DROPPED_TABLE_NAME_TO_COLUMNS_PREFIX).Find();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="user"></param>
        /// <param name="columnTypes"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        /// <exception cref="ArgumentException"></exception>
        public static void CreateTable(UInt160 user, ByteString tableName, ByteString columnTypes)
        {
            ExecutionEngine.Assert(Runtime.CheckWitness(user), "witness CreateTable");
            ExecutionEngine.Assert(StdLib.MemorySearch(tableName, (ByteString)new byte[] { SEPARATOR }) == -1,
                "SEPARATOR in tableName");
            if (columnTypes.Length > 256) throw new ArgumentOutOfRangeException("Too many columns");
            if (columnTypes.Length == 0) throw new ArgumentException("No column specified");
            int l = columnTypes.Length;
            for (int i = 0; i < l; ++i)
            {
                byte type = columnTypes[i];
                if (type == BOOLEAN || type == INT_VAR_LEN || type == BYTESTRING_VAR_LEN)
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
            new StorageMap(context, TABLE_ROW_ID_PREFIX).Put(key, 1);
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

        public static void AddRow(UInt160 user, ByteString tableName, object[] row)
        {
            ExecutionEngine.Assert(Runtime.CheckWitness(user), "witness AddRow");
            StorageContext context = Storage.CurrentContext;
            ByteString key = user + tableName;
            ByteString columnTypes = new StorageMap(context, USER_TABLE_NAME_TO_COLUMNS_PREFIX)[key];
            BigInteger columnTypesLength = columnTypes.Length;
            BigInteger rowLength = row.Length;
            int columnTypesIndexer = 0;
            int rowIndexer = 0;
            ByteString writeContent = "";
            for ( ; columnTypesIndexer < columnTypesLength; columnTypesIndexer++)
            {
                ExecutionEngine.Assert(rowIndexer < rowLength, "Wrong count of columns: rowIndexer=" + rowIndexer);
                byte type = columnTypes[columnTypesIndexer];
                switch (type)
                {
                    case BOOLEAN:
                        writeContent += (bool)row[rowIndexer] ? "\x01" : "\x00";
                        break;
                    case INT_VAR_LEN:
                    case BYTESTRING_VAR_LEN:
                        writeContent += EncodeByteString((ByteString)row[rowIndexer]);
                        break;
                    case INT_FIXED_LEN:
                    case BYTESTRING_FIXED_LEN:
                        byte dataLength = columnTypes[++columnTypesIndexer];
                        ByteString data = (ByteString)row[rowIndexer];
                        writeContent += EncodeByteStringFixedLength(data, dataLength);
                        break;
                    default:
                        throw new ArgumentException("Unsupported type " +  type);
                }
                ++rowIndexer;
            }
            StorageMap tableRowId = new StorageMap(context, TABLE_ROW_ID_PREFIX);
            ByteString rowId = tableRowId[key];
            new StorageMap(context, ROWS_PREFIX).Put(key + SEPARATOR + rowId, writeContent);
            tableRowId.Put(key, (BigInteger)rowId + 1);
        }

        public static void DeleteRow(UInt160 user, ByteString tableName, BigInteger rowId)
        {
            ExecutionEngine.Assert(Runtime.CheckWitness(user), "witness DeleteRow");
            new StorageMap(ROWS_PREFIX).Delete(user + tableName + SEPARATOR + rowId);
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
                suffix += "\x00";
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

        public static ByteString EncodeIntegerFixedLength(BigInteger data, BigInteger fixedLength) => EncodeByteStringFixedLength((ByteString)data, fixedLength);
        public static ByteString EncodeByteStringFixedLength(ByteString data, BigInteger fixedLength)
        {
            BigInteger lengthToComplement = fixedLength - data.Length;
            if (lengthToComplement < 0)
                throw new ArgumentException("Too long value " + data);
            ByteString encoded = data;
            if (lengthToComplement > 0)
            {
                ByteString suffix = "";
                for (; lengthToComplement > 0; --lengthToComplement)
                    suffix += "\x00";
                encoded += suffix;
            }
            return encoded;
        }
    }
}
