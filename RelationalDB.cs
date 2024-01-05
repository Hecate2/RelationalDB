using System;
using System.ComponentModel;
using System.Numerics;
using Neo.SmartContract.Framework;
using Neo.SmartContract.Framework.Attributes;

namespace SecondaryIndex
{
    [DisplayName("RelationalDB")]
    [ManifestExtra("Author", "Hecate2")]
    [ManifestExtra("Email", "developer@neo.org")]
    [ManifestExtra("Description", "This is a SecondaryIndex")]
    public class RelationalDB : SmartContract
    {
        const int LengthOfLength = 8;
        public enum StackItemType : byte
        {
            Any = 0x00,  // also for NULL
            Pointer = 0x10,
            Boolean = 0x20,
            Integer = 0x21,
            ByteString = 0x28,
            Buffer = 0x30,
            Array = 0x40,
            Struct = 0x41,
            Map = 0x48,
            InteropInterface = 0x60,
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
            if (lengthToComplement < 0) throw new ArgumentException("Integer too large");
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
