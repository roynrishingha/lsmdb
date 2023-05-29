use std::{
    fs::read_dir,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

/// Generates current time as micro-seconds
pub fn generate_timestamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("failed to generate timestamp")
        .as_micros()
}

/// Gets the set of files with a given extension from a given directory as a Vector of path buffers
pub fn get_files_with_ext(dir: &Path, ext: &str) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for file in read_dir(dir).expect("No directory found") {
        let path = file.unwrap().path();
        if path.extension().unwrap() == ext {
            files.push(path);
        }
    }

    files
}
