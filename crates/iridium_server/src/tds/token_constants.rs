// TDS token type constants
pub const COLMETADATA_TOKEN: u8 = 0x81;
pub const ROW_TOKEN: u8 = 0xD1;
pub const DONE_TOKEN: u8 = 0xFD;
pub const DONEINPROC_TOKEN: u8 = 0xFF;
pub const DONEPROC_TOKEN: u8 = 0xFE;
pub const ERROR_TOKEN: u8 = 0xAA;
pub const INFO_TOKEN: u8 = 0xAB;
pub const LOGINACK_TOKEN: u8 = 0xAD;
pub const ENVCHANGE_TOKEN: u8 = 0xE3;
pub const FEATUREEXTACK_TOKEN: u8 = 0xAE;
pub const RETURNSTATUS_TOKEN: u8 = 0x79;
pub const OUTPUT_PARAM_TOKEN: u8 = 0x80;

// DONE status flags
pub const DONE_FINAL: u16 = 0x0000;
pub const DONE_MORE: u16 = 0x0001;
pub const DONE_ERROR: u16 = 0x0002;
pub const DONE_COUNT: u16 = 0x0010;
pub const DONE_ATTN: u16 = 0x0020;

// ENVCHANGE types
pub const ENVCHANGE_PACKET_SIZE: u8 = 0x04;
pub const ENVCHANGE_DATABASE: u8 = 0x01;
pub const ENVCHANGE_LANGUAGE_TYPE: u8 = 0x02;
pub const ENVCHANGE_COLLATION: u8 = 0x07;
