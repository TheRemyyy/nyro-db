use crate::config::LoggingConfig;
use chrono::Timelike;

pub struct Logger;

#[allow(dead_code)]
impl Logger {
    pub fn info(msg: &str) {
        Self::log("INFO", "\x1b[32m", msg);
    }

    pub fn error(msg: &str) {
        Self::log("ERROR", "\x1b[31m", msg);
    }

    pub fn warn(msg: &str) {
        Self::log("WARN", "\x1b[33m", msg);
    }

    pub fn shutdown(msg: &str) {
        Self::log("SHUTDOWN", "\x1b[33m", msg);
    }

    fn log(level: &str, color: &str, msg: &str) {
        let now = chrono::Local::now();
        println!(
            "\x1b[90m{}:{:02}\x1b[0m {}{}\x1b[0m \x1b[90mNyroDB:\x1b[0m \x1b[37m{}\x1b[0m",
            now.hour(),
            now.minute(),
            color,
            level,
            msg
        );
    }

    pub fn info_with_config(config: &LoggingConfig, msg: &str) {
        if Self::should_log(config, "info") {
            if config.enable_colors {
                Self::info(msg);
            } else {
                Self::log_plain("INFO", msg);
            }
        }
    }

    pub fn error_with_config(config: &LoggingConfig, msg: &str) {
        if Self::should_log(config, "error") {
            if config.enable_colors {
                Self::error(msg);
            } else {
                Self::log_plain("ERROR", msg);
            }
        }
    }

    pub fn warn_with_config(config: &LoggingConfig, msg: &str) {
        if Self::should_log(config, "warn") {
            if config.enable_colors {
                Self::warn(msg);
            } else {
                Self::log_plain("WARN", msg);
            }
        }
    }

    pub fn shutdown_with_config(config: &LoggingConfig, msg: &str) {
        if Self::should_log(config, "shutdown") {
            if config.enable_colors {
                Self::shutdown(msg);
            } else {
                Self::log_plain("SHUTDOWN", msg);
            }
        }
    }

    fn should_log(config: &LoggingConfig, level: &str) -> bool {
        match config.level.as_str() {
            "error" => level == "error" || level == "shutdown",
            "warn" => level == "error" || level == "warn" || level == "shutdown",
            "info" | "shutdown" => true,
            _ => true,
        }
    }

    fn log_plain(level: &str, msg: &str) {
        let now = chrono::Local::now();
        println!(
            "{}:{:02} {} NyroDB: {}",
            now.hour(),
            now.minute(),
            level,
            msg
        );
    }
}
