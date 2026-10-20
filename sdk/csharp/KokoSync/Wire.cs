using System.Buffers;
using MessagePack;

namespace KokoSync;

/// <summary>String-map MessagePack codec. Protocol counters remain UInt64.</summary>
public static class Wire
{
    public const int MaxFrame = 65536;
    public const int MaxRealtimeFrame = 1000;
    public static byte[] Encode(IReadOnlyDictionary<string, object?> value, int maxBytes = MaxFrame)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);
        Write(ref writer, value, 0);
        writer.Flush();
        if (buffer.WrittenCount > maxBytes) throw new ArgumentException($"Frame exceeds {maxBytes} bytes.");
        return buffer.WrittenSpan.ToArray();
    }

    public static Dictionary<string, object?> Decode(ReadOnlyMemory<byte> bytes, int maxBytes = MaxFrame)
    {
        if (bytes.Length > maxBytes) throw new ArgumentException($"Frame exceeds {maxBytes} bytes.");
        var reader = new MessagePackReader(bytes);
        var result = Read(ref reader, 0) as Dictionary<string, object?>
            ?? throw new FormatException("Frame must be a string-key map.");
        if (!reader.End) throw new FormatException("Trailing MessagePack data.");
        return result;
    }

    public static void ValidateValue(object? value, int maxBytes = 16384)
    {
        Validate(value, 0);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);
        Write(ref writer, value, 0);
        writer.Flush();
        if (buffer.WrittenCount > maxBytes) throw new ArgumentException($"Application value exceeds {maxBytes} bytes.");
    }
    internal static byte[] EncodeValue(object? value, int maxBytes)
    {
        Validate(value, 0);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);
        Write(ref writer, value, 0);
        writer.Flush();
        if (buffer.WrittenCount > maxBytes) throw new ValueTooLargeException(maxBytes);
        return buffer.WrittenSpan.ToArray();
    }
    internal sealed class ValueTooLargeException(int maxBytes)
        : ArgumentException($"Application value exceeds {maxBytes} bytes.");
    internal static object? DecodeValue(ReadOnlyMemory<byte> bytes)
    {
        var reader = new MessagePackReader(bytes);
        var value = Read(ref reader, 0);
        if (!reader.End) throw new FormatException("Trailing MessagePack data.");
        Validate(value, 0);
        return value;
    }

    private static void Validate(object? value, int depth)
    {
        if (depth > 32) throw new ArgumentException("Application value nesting exceeds 32.");
        switch (value)
        {
            case null or bool or string or byte[]: return;
            case byte or sbyte or short or ushort or int or uint: return;
            case long n when n >= -9007199254740991L && n <= 9007199254740991L: return;
            case ulong n when n <= 9007199254740991UL: return;
            case float n when float.IsFinite(n) && Math.Abs(n) <= 9007199254740991d: return;
            case double n when double.IsFinite(n) && Math.Abs(n) <= 9007199254740991d: return;
            case IReadOnlyDictionary<string, object?> map:
                foreach (var item in map.Values) Validate(item, depth + 1);
                return;
            case object?[] array:
                foreach (var item in array) Validate(item, depth + 1);
                return;
            default: throw new ArgumentException("Unsupported or unsafe application value.");
        }
    }

    private static void Write(ref MessagePackWriter writer, object? value, int depth)
    {
        if (depth > 36) throw new ArgumentException("Message nesting limit exceeded.");
        switch (value)
        {
            case null: writer.WriteNil(); break;
            case bool n: writer.Write(n); break;
            case string n: writer.Write(n); break;
            case byte[] n: writer.Write(n); break;
            case byte n: writer.Write(n); break;
            case sbyte n: writer.Write(n); break;
            case short n: writer.Write(n); break;
            case ushort n: writer.Write(n); break;
            case int n: writer.Write(n); break;
            case uint n: writer.Write(n); break;
            case long n: writer.Write(n); break;
            case ulong n: writer.WriteUInt64(n); break;
            case float n when float.IsFinite(n): writer.Write(n); break;
            case double n when double.IsFinite(n): writer.Write(n); break;
            case IReadOnlyDictionary<string, object?> map:
                writer.WriteMapHeader(map.Count);
                foreach (var item in map) { writer.Write(item.Key); Write(ref writer, item.Value, depth + 1); }
                break;
            case object?[] array:
                writer.WriteArrayHeader(array.Length);
                foreach (var item in array) Write(ref writer, item, depth + 1);
                break;
            default: throw new ArgumentException("Unsupported MessagePack value.");
        }
    }

    private static object? Read(ref MessagePackReader reader, int depth)
    {
        if (depth > 36) throw new FormatException("Message nesting limit exceeded.");
        switch (reader.NextMessagePackType)
        {
            case MessagePackType.Nil: reader.ReadNil(); return null;
            case MessagePackType.Boolean: return reader.ReadBoolean();
            case MessagePackType.String: return reader.ReadString();
            case MessagePackType.Binary: return reader.ReadBytes()?.ToArray();
            case MessagePackType.Float: return reader.ReadDouble();
            case MessagePackType.Integer:
                var code = reader.NextCode;
                return code <= 0x7f || code is 0xcc or 0xcd or 0xce or 0xcf
                    ? reader.ReadUInt64() : (object)reader.ReadInt64();
            case MessagePackType.Array:
                var count = reader.ReadArrayHeader();
                if (count > reader.Sequence.Length - reader.Consumed) throw new FormatException("Invalid array length.");
                var array = new object?[count];
                for (var i = 0; i < count; i++) array[i] = Read(ref reader, depth + 1);
                return array;
            case MessagePackType.Map:
                var length = reader.ReadMapHeader();
                if (length > (reader.Sequence.Length - reader.Consumed) / 2) throw new FormatException("Invalid map length.");
                var map = new Dictionary<string, object?>(StringComparer.Ordinal);
                for (var i = 0; i < length; i++)
                {
                    var key = reader.ReadString() ?? throw new FormatException("Null map key.");
                    if (!map.TryAdd(key, Read(ref reader, depth + 1))) throw new FormatException("Duplicate map key.");
                }
                return map;
            default: throw new FormatException("Unsupported MessagePack type.");
        }
    }
}
