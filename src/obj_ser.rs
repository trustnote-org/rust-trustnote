use serde::ser::{self, Serialize};
use std::collections::BTreeMap;
use std::error;
use std::fmt;

pub type Result<T> = ::std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Custom(String),
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Custom(ref s) => s.fmt(fmt),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Custom(ref msg) => msg,
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        None
    }
}

impl ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Error::Custom(msg.to_string())
    }
}

pub struct Serializer {
    // This string starts empty and JSON is appended as values are serialized.
    output: Vec<String>,
    size: usize,
}

pub struct StructSerializer<'a> {
    s: &'a mut Serializer,
    fields: BTreeMap<&'static str, Vec<String>>,
    size: usize,
}

pub struct MapSerializer<'a> {
    s: &'a mut Serializer,
    entries: BTreeMap<String, Vec<String>>,
    last_key: Option<String>,
    size: usize,
}

// By convention, the public API of a Serde deserializer is one or more `to_abc`
// functions such as `to_string`, `to_bytes`, or `to_writer` depending on what
// Rust types the serializer is able to produce as output.
//
// This basic serializer supports only `to_string`.
pub fn to_string<T>(value: &T) -> Result<String>
where
    T: Serialize,
{
    let mut serializer = Serializer {
        output: Vec::new(),
        size: 0,
    };
    value.serialize(&mut serializer)?;
    Ok(serializer.output.join("\u{0000}"))
}

// get object size (special with trustnote)
#[allow(dead_code)]
pub fn obj_size<T>(value: &T) -> Result<usize>
where
    T: Serialize,
{
    let mut serializer = Serializer {
        output: Vec::new(),
        size: 0,
    };
    value.serialize(&mut serializer)?;
    Ok(serializer.size)
}

impl<'a> ser::Serializer for &'a mut Serializer {
    // The output type produced by this `Serializer` during successful
    // serialization. Most serializers that produce text or binary output should
    // set `Ok = ()` and serialize into an `io::Write` or buffer contained
    // within the `Serializer` instance, as happens here. Serializers that build
    // in-memory data structures may be simplified by using `Ok` to propagate
    // the data structure around.
    type Ok = ();

    // The error type when some error occurs during serialization.
    type Error = Error;

    // Associated types for keeping track of additional state while serializing
    // compound data structures like sequences and maps. In this case no
    // additional state is required beyond what is already stored in the
    // Serializer struct.
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = MapSerializer<'a>;
    type SerializeStruct = StructSerializer<'a>;
    type SerializeStructVariant = Self;

    // Here we go with the simple methods. The following 12 methods receive one
    // of the primitive types of the data model and map it to JSON by appending
    // into the output string.
    fn serialize_bool(self, v: bool) -> Result<()> {
        let value = if v { "true" } else { "false" };
        self.output.push("b".to_string());
        self.output.push(value.to_string());
        self.size += 1;
        Ok(())
    }

    // JSON does not distinguish between different sizes of integers, so all
    // signed integers will be serialized the same and all unsigned integers
    // will be serialized the same. Other formats, especially compact binary
    // formats, may need independent logic for the different sizes.
    fn serialize_i8(self, v: i8) -> Result<()> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        self.serialize_i64(i64::from(v))
    }

    // Not particularly efficient but this is example code anyway. A more
    // performant approach would be to use the `itoa` crate.
    fn serialize_i64(self, v: i64) -> Result<()> {
        self.output.push("n".to_string());
        self.output.push(v.to_string());
        self.size += 8;
        Ok(())
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        self.output.push("n".to_string());
        self.output.push(v.to_string());
        self.size += 8;
        Ok(())
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        self.serialize_f64(f64::from(v))
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        self.output.push("n".to_string());
        self.output.push(v.to_string());
        self.size += 8;
        Ok(())
    }

    // Serialize a char as a single-character string. Other formats may
    // represent this differently.
    fn serialize_char(self, v: char) -> Result<()> {
        self.serialize_str(&v.to_string())
    }

    // This only works for strings that don't require escape sequences but you
    // get the idea. For example it would emit invalid JSON if the input string
    // contains a '"' character.
    fn serialize_str(self, v: &str) -> Result<()> {
        self.output.push("s".to_string());
        self.output.push(v.to_string());
        // we use unicode characters len
        self.size += v.chars().count();
        Ok(())
    }

    // Serialize a byte array as an array of bytes. Could also use a base64
    // string here. Binary formats will typically represent byte arrays more
    // compactly.
    fn serialize_bytes(self, _v: &[u8]) -> Result<()> {
        Err(Error::Custom("serialize bytes not supported".to_string()))
    }

    // An absent optional is represented as the JSON `null`.
    fn serialize_none(self) -> Result<()> {
        // self.serialize_unit()
        self.output.push("null".to_string());
        Ok(())
    }

    // A present optional is represented as just the contained value. Note that
    // this is a lossy representation. For example the values `Some(())` and
    // `None` both serialize as just `null`. Unfortunately this is typically
    // what people expect when working with JSON. Other formats are encouraged
    // to behave more intelligently if possible.
    fn serialize_some<T>(self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    // In Serde, unit means an anonymous value containing no data. Map this to
    // JSON as `null`.
    fn serialize_unit(self) -> Result<()> {
        self.output.push("null".to_string());
        Ok(())
        // Err(Error::Custom("serialize () not supported".to_string()))
    }

    // Unit struct means a named value containing no data. Again, since there is
    // no data, map this to JSON as `null`. There is no need to serialize the
    // name in most formats.
    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        self.serialize_unit()
    }

    // When serializing a unit variant (or any other kind of variant), formats
    // can choose whether to keep track of it by index or by name. Binary
    // formats typically use the index of the variant and human-readable formats
    // typically use the name.
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<()> {
        self.serialize_str(variant)
    }

    // As is done here, serializers are encouraged to treat newtype structs as
    // insignificant wrappers around the data they contain.
    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    // Note that newtype variant (and all of the other variant serialization
    // methods) refer exclusively to the "externally tagged" enum
    // representation.
    //
    // Serialize this to JSON in externally tagged form as `{ NAME: VALUE }`.
    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        variant.serialize(&mut *self)?;
        value.serialize(&mut *self)?;
        Ok(())
    }

    // Now we get to the serialization of compound types.
    //
    // The start of the sequence, each value, and the end are three separate
    // method calls. This one is responsible only for serializing the start,
    // which in JSON is `[`.
    //
    // The length of the sequence may or may not be known ahead of time. This
    // doesn't make a difference in JSON because the length is not represented
    // explicitly in the serialized form. Some serializers may only be able to
    // support sequences for which the length is known up front.
    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        self.output.push("[".to_string());
        Ok(self)
    }

    // Tuples look just like sequences in JSON. Some formats may be able to
    // represent tuples more efficiently by omitting the length, since tuple
    // means that the corresponding `Deserialize implementation will know the
    // length without needing to look at the serialized data.
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        self.serialize_seq(Some(len))
    }

    // Tuple structs look just like sequences in JSON.
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.serialize_seq(Some(len))
    }

    // Tuple variants are represented in JSON as `{ NAME: [DATA...] }`. Again
    // this method is only responsible for the externally tagged representation.
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        variant.serialize(&mut *self)?;
        self.output.push("[".to_string());
        Ok(self)
    }

    // Maps are represented in JSON as `{ K: V, K: V, ... }`.
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        Ok(MapSerializer {
            s: self,
            entries: BTreeMap::new(),
            last_key: None,
            size: 0,
        })
    }

    // Structs look just like maps in JSON. In particular, JSON requires that we
    // serialize the field names of the struct. Other formats may be able to
    // omit the field names when serializing structs because the corresponding
    // Deserialize implementation is required to know what the keys are without
    // looking at the serialized data.
    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Ok(StructSerializer {
            s: self,
            fields: BTreeMap::new(),
            size: 0,
        })
    }

    // Struct variants are represented in JSON as `{ NAME: { K: V, ... } }`.
    // This is the externally tagged representation.
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        variant.serialize(&mut *self)?;
        Ok(self)
    }
}

// The following 7 impls deal with the serialization of compound types like
// sequences and maps. Serialization of such types is begun by a Serializer
// method and followed by zero or more calls to serialize individual elements of
// the compound type and one call to end the compound type.
//
// This impl is SerializeSeq so these methods are called after `serialize_seq`
// is called on the Serializer.
impl<'a> ser::SerializeSeq for &'a mut Serializer {
    // Must match the `Ok` type of the serializer.
    type Ok = ();
    // Must match the `Error` type of the serializer.
    type Error = Error;

    // Serialize a single element of the sequence.
    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    // Close the sequence.
    fn end(self) -> Result<()> {
        self.output.push("]".to_string());
        Ok(())
    }
}

// Same thing but for tuples.
impl<'a> ser::SerializeTuple for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.output.push("]".to_string());
        Ok(())
    }
}

// Same thing but for tuple structs.
impl<'a> ser::SerializeTupleStruct for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.output.push("]".to_string());
        Ok(())
    }
}

// Tuple variants are a little different. Refer back to the
// `serialize_tuple_variant` method above:
//
//    self.output += "{";
//    variant.serialize(&mut *self)?;
//    self.output += ":[";
//
// So the `end` method in this impl is responsible for closing both the `]` and
// the `}`.
impl<'a> ser::SerializeTupleVariant for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        self.output.push("]".to_string());
        Ok(())
    }
}

// Some `Serialize` types are not able to hold a key and value in memory at the
// same time so `SerializeMap` implementations are required to support
// `serialize_key` and `serialize_value` individually.
//
// There is a third optional method on the `SerializeMap` trait. The
// `serialize_entry` method allows serializers to optimize for the case where
// key and value are both available simultaneously. In JSON it doesn't make a
// difference so the default behavior for `serialize_entry` is fine.
impl<'a> ser::SerializeMap for MapSerializer<'a> {
    type Ok = ();
    type Error = Error;

    // The Serde data model allows map keys to be any serializable type. JSON
    // only allows string keys so the implementation below will produce invalid
    // JSON if the key serializes as something other than a string.
    //
    // A real JSON serializer would need to validate that map keys are strings.
    // This can be done by using a different Serializer to serialize the key
    // (instead of `&mut **self`) and having that other serializer only
    // implement `serialize_str` and return an error on any other data type.
    fn serialize_key<T>(&mut self, key: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        // only string key is supported, how to do that?
        let mut serializer = Serializer {
            output: Vec::new(),
            size: 0,
        };
        key.serialize(&mut serializer)?;
        if serializer.output.len() != 2 || serializer.output[0] != "s" {
            return Err(Error::Custom("only string key map supported".to_string()));
        }

        let key = serializer.output[1].clone();
        self.last_key = Some(key.clone());
        self.entries.insert(key, Vec::new());
        Ok(())
    }

    // It doesn't make a difference whether the colon is printed at the end of
    // `serialize_key` or at the beginning of `serialize_value`. In this case
    // the code is a bit simpler having it here.
    fn serialize_value<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        let mut serializer = Serializer {
            output: Vec::new(),
            size: 0,
        };
        value.serialize(&mut serializer)?;
        let mut value = serializer.output;
        let key = self.last_key.take().unwrap();
        if let Some(v) = self.entries.get_mut(&key) {
            v.append(&mut value);
        }
        self.size += serializer.size;
        Ok(())
    }

    fn serialize_entry<K: ?Sized, V: ?Sized>(&mut self, key: &K, value: &V) -> Result<()>
    where
        K: Serialize,
        V: Serialize,
    {
        // only string key is supported, how to do that?
        let mut serializer = Serializer {
            output: Vec::new(),
            size: 0,
        };
        key.serialize(&mut serializer)?;
        if serializer.output.len() != 2 || serializer.output[0] != "s" {
            return Err(Error::Custom("only string key map supported".to_string()));
        }
        let key = serializer.output[1].clone();

        let mut serializer = Serializer {
            output: Vec::new(),
            size: 0,
        };
        value.serialize(&mut serializer)?;
        let value = serializer.output;

        self.entries.insert(key, value);
        self.size += serializer.size;
        Ok(())
    }

    fn end(self) -> Result<()> {
        for (k, mut v) in self.entries {
            // filter out null entry
            if v[0] == "null" {
                continue;
            }
            self.s.output.push(k.to_owned());
            self.s.output.append(&mut v);
        }
        self.s.size += self.size;
        Ok(())
    }
}

// Structs are like maps in which the keys are constrained to be compile-time
// constant strings.
impl<'a> ser::SerializeStruct for StructSerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        // filter out null
        let mut serializer = Serializer {
            output: Vec::new(),
            size: 0,
        };
        value.serialize(&mut serializer)?;
        if serializer.output == ["null"] {
            return Ok(());
        }

        self.fields.insert(key, serializer.output);
        self.size += serializer.size;
        Ok(())
    }

    fn end(self) -> Result<()> {
        for (k, mut v) in self.fields {
            self.s.output.push(k.to_owned());
            self.s.output.append(&mut v);
        }
        self.s.size += self.size;
        Ok(())
    }
}

// Similar to `SerializeTupleVariant`, here the `end` method is responsible for
// closing both of the curly braces opened by `serialize_struct_variant`.
impl<'a> ser::SerializeStructVariant for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        key.serialize(&mut **self)?;
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_struct() {
    #[derive(Serialize)]
    struct Test {
        int: u32,
        flag: bool,
        none: Option<u32>, // if its none, not affect hash
        seq: Vec<&'static str>,
    }

    let test = Test {
        int: 1,
        flag: false,
        none: None,
        seq: vec!["a", "b"],
    };
    let expected =
        "flag\u{0}b\u{0}false\u{0}int\u{0}n\u{0}1\u{0}seq\u{0}[\u{0}s\u{0}a\u{0}s\u{0}b\u{0}]";
    assert_eq!(to_string(&test).unwrap(), expected);
}

#[test]
fn test_map() {
    use std::collections::HashMap;
    let mut map = HashMap::new();
    map.insert("unit", "rg1RzwKwnfRHjBojGol3gZaC5w7kR++rOR6O61JRsrQ=");
    map.insert("aaaa", "some value");
    let expected =
        "aaaa\u{0}s\u{0}some value\u{0}unit\u{0}s\u{0}rg1RzwKwnfRHjBojGol3gZaC5w7kR++rOR6O61JRsrQ=";
    assert_eq!(to_string(&map).unwrap(), expected);
}

#[test]
fn test_json_value() {
    let v = json!({
      "name": "John Doe",
      "age": 43,
      "phones": [
        "+44 1234567",
      ]
    });

    let expected =
        "age\u{0}n\u{0}43\u{0}name\u{0}s\u{0}John Doe\u{0}phones\u{0}[\u{0}s\u{0}+44 1234567\u{0}]";
    assert_eq!(to_string(&v).unwrap(), expected);
}

#[test]
fn test_json_size() {
    let v = json!({
      "name": "John Doe",
      "age": 43,
      "phones": [
        "+44 1234567",
      ]
    });

    assert_eq!(obj_size(&v).unwrap(), 27);
}
