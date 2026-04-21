#[path = "common.rs"]
mod common;
#[path = "data_types.rs"]
mod data_types;
#[path = "pratt.rs"]
mod pratt;
#[path = "primary.rs"]
mod primary;
#[path = "window.rs"]
mod window;

use crate::parser::ast::*;
use crate::parser::error::ParseResult;
use crate::parser::state::Parser;

pub use common::{is_stop_keyword, parse_comma_list};
pub use data_types::parse_data_type;
#[allow(unused_imports)]
pub use primary::{parse_case, parse_cast, parse_convert, parse_primary, parse_try_cast, parse_try_convert};

pub fn parse_expr(parser: &mut Parser) -> ParseResult<Expr> {
    pratt::parse_expr(parser)
}
