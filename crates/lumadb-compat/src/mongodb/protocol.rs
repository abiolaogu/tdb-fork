//! MongoDB wire protocol implementation

use bytes::{Buf, BufMut, BytesMut};
use std::io::{self, Read, Write};

use super::types::*;

/// Parse MongoDB message header
pub fn parse_header(data: &[u8]) -> io::Result<MsgHeader> {
    if data.len() < 16 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Header too short"));
    }

    let mut cursor = std::io::Cursor::new(data);
    let mut buf = [0u8; 4];

    cursor.read_exact(&mut buf)?;
    let message_length = i32::from_le_bytes(buf);

    cursor.read_exact(&mut buf)?;
    let request_id = i32::from_le_bytes(buf);

    cursor.read_exact(&mut buf)?;
    let response_to = i32::from_le_bytes(buf);

    cursor.read_exact(&mut buf)?;
    let op_code = u32::from_le_bytes(buf).into();

    Ok(MsgHeader {
        message_length,
        request_id,
        response_to,
        op_code,
    })
}

/// Serialize MongoDB message header
pub fn serialize_header(header: &MsgHeader, buf: &mut BytesMut) {
    buf.put_i32_le(header.message_length);
    buf.put_i32_le(header.request_id);
    buf.put_i32_le(header.response_to);
    buf.put_u32_le(header.op_code as u32);
}

/// Parse OP_MSG message
pub fn parse_op_msg(data: &[u8]) -> io::Result<bson::Document> {
    if data.len() < 5 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "OP_MSG too short"));
    }

    let _flags = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    let section_kind = data[4];

    if section_kind != SectionKind::Body as u8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Unsupported section kind: {}", section_kind),
        ));
    }

    // Parse BSON document starting at position 5
    let doc_data = &data[5..];
    bson::Document::from_reader(&mut std::io::Cursor::new(doc_data))
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

/// Serialize OP_MSG response
pub fn serialize_op_msg(request_id: i32, response_to: i32, doc: &bson::Document) -> BytesMut {
    let mut body = Vec::new();
    doc.to_writer(&mut body).expect("Failed to serialize BSON");

    let message_length = 16 + 4 + 1 + body.len() as i32; // header + flags + section kind + body

    let mut buf = BytesMut::with_capacity(message_length as usize);

    // Header
    serialize_header(
        &MsgHeader {
            message_length,
            request_id,
            response_to,
            op_code: OpCode::OpMsg,
        },
        &mut buf,
    );

    // Flags (none set)
    buf.put_u32_le(0);

    // Section kind (Body = 0)
    buf.put_u8(SectionKind::Body as u8);

    // BSON document
    buf.extend_from_slice(&body);

    buf
}

/// Parse legacy OP_QUERY message (for older clients)
pub fn parse_op_query(data: &[u8]) -> io::Result<(String, bson::Document)> {
    if data.len() < 12 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "OP_QUERY too short"));
    }

    let mut cursor = std::io::Cursor::new(data);

    // Skip flags
    let mut buf = [0u8; 4];
    cursor.read_exact(&mut buf)?;

    // Read collection name (null-terminated string)
    let mut collection = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        cursor.read_exact(&mut byte)?;
        if byte[0] == 0 {
            break;
        }
        collection.push(byte[0]);
    }
    let collection_name = String::from_utf8_lossy(&collection).to_string();

    // Skip numberToSkip and numberToReturn
    cursor.read_exact(&mut buf)?;
    cursor.read_exact(&mut buf)?;

    // Read query document
    let remaining: Vec<u8> = cursor.bytes().filter_map(Result::ok).collect();
    let query = bson::Document::from_reader(&mut std::io::Cursor::new(&remaining))
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

    Ok((collection_name, query))
}

/// Convert BSON document to JSON-compatible HashMap
pub fn bson_to_json(doc: &bson::Document) -> serde_json::Value {
    serde_json::to_value(doc).unwrap_or(serde_json::Value::Null)
}

/// Convert JSON value to BSON document
pub fn json_to_bson(value: &serde_json::Value) -> bson::Document {
    match bson::to_document(value) {
        Ok(doc) => doc,
        Err(_) => bson::Document::new(),
    }
}
