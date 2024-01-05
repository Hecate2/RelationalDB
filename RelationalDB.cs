using System;
using System.ComponentModel;
using System.Numerics;
using Neo;
using Neo.SmartContract.Framework;
using Neo.SmartContract.Framework.Attributes;
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

        const byte TYPE_BOOLEAN = 0x20;
        const byte TYPE_INTEGER_VAR_LENGTH = 0x21;
        const byte TYPE_BYTESTRING_VAR_LENGTH = 0x28;
        const byte TYPE_INTEGER_FIXED_LENGTH = 0x31;
        const byte TYPE_BYTESTRING_FIXED_LENGTH = 0x38;
        //const byte TYPE_ARRAY = 0x40;
        //const byte TYPE_STRUCT = 0x41;
        //const byte TYPE_MAP = 0x48;

        const byte USER_TO_TABLE_ID_PREFIX = (byte)'U';  // 0x55
        const byte USER_TABLE_ID_TO_COLUMNS_PREFIX = (byte)'T';  // 0x54
        const byte DROPPED_TABLE_ID_TO_COLUMNS_PREFIX = (byte)'D';  // 0x44

        /// <summary>
        /// 
        /// </summary>
        /// <param name="user"></param>
        /// <param name="columnTypes"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        /// <exception cref="ArgumentException"></exception>
        public static BigInteger CreateTable(UInt160 user, ByteString columnTypes)
        {
            ExecutionEngine.Assert(Runtime.CheckWitness(user), "witness CreateTable");
            if (columnTypes.Length > 256) throw new ArgumentOutOfRangeException("Too many columns");
            if (columnTypes.Length == 0) throw new ArgumentException("No column specified");
            int l = columnTypes.Length;
            for (int i = 0; i < l; ++i)
            {
                byte type = columnTypes[i];
                if (type == TYPE_BOOLEAN || type != TYPE_INTEGER_VAR_LENGTH || type == TYPE_BYTESTRING_VAR_LENGTH)
                    continue;
                if (type == TYPE_INTEGER_FIXED_LENGTH || type != TYPE_BYTESTRING_FIXED_LENGTH)
                {
                    ++i;
                    // now columnTypes[i] refers to the (fixed) length of the value, in count of bytes
                    continue;
                }
                throw new ArgumentException("Invalid type " + type);
            }

            StorageContext context = Storage.CurrentContext;
            StorageMap userToTableId = new(context, USER_TO_TABLE_ID_PREFIX);
            ByteString byteStringId = userToTableId.Get(user);
            BigInteger bigIntegerId = (BigInteger)byteStringId;
            userToTableId.Put(user, bigIntegerId + 1);
            new StorageMap(context, USER_TABLE_ID_TO_COLUMNS_PREFIX).Put(user + byteStringId, columnTypes);
            return bigIntegerId;
        }

        public static void DropTable(UInt160 user, ByteString tableId)
        {
            ExecutionEngine.Assert(Runtime.CheckWitness(user), "witness DropTable");
            StorageContext context = Storage.CurrentContext;
            ByteString key = user + tableId;
            StorageMap userToTableId = new StorageMap(context, USER_TABLE_ID_TO_COLUMNS_PREFIX);
            ByteString columnTypes = userToTableId.Get(key);
            if (columnTypes == null || columnTypes.Length == 0)
                throw new ArgumentException("No table at id " + tableId);
            userToTableId.Delete(key);
            new StorageMap(context, DROPPED_TABLE_ID_TO_COLUMNS_PREFIX).Put(key, columnTypes);
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
    }
}
