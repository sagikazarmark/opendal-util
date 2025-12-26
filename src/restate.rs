use opendal::ErrorKind;
use restate_sdk::errors::{HandlerError, TerminalError};

pub fn to_restate_error(err: opendal::Error) -> HandlerError {
    if err.is_permanent() {
        let status_code = match err.kind() {
            ErrorKind::Unsupported => 501,
            ErrorKind::ConfigInvalid => 400,
            ErrorKind::NotFound => 404,
            ErrorKind::PermissionDenied => 403,
            ErrorKind::IsADirectory => 422,
            ErrorKind::NotADirectory => 422,
            ErrorKind::AlreadyExists => 409,
            _ => 500,
        };

        return TerminalError::new_with_code(status_code, err.to_string()).into();
    }

    err.into()
}
