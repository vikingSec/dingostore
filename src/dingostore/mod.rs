use std::{collections::BTreeMap, fmt::{Debug, Display, Formatter}, time::{SystemTime, UNIX_EPOCH}};
use std::any::Any;
use std::mem::size_of_val;
use std::fs::{File, OpenOptions};
use std::io::{Write, BufReader, Read};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

const SIZE_THRESH: u32 = 80000;

pub trait Serializable: Debug + Display {
    fn serialize(&self) -> Vec<u8>;
    fn deserialize(bytes: &[u8]) -> Self where Self: Sized;
}

impl Serializable for String {
    fn serialize(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
    fn deserialize(bytes: &[u8]) -> Self {
        String::from_utf8(bytes.to_vec()).unwrap()
    }
}

pub struct DingoStore<'a> {
    objs: BTreeMap<u64, String>,
    keys: Vec<u64>,
    fname: &'a str,
    treesize: u32,
    flushed_files: Arc<Mutex<BTreeMap<String, String>>>,
    index: Arc<Mutex<BTreeMap<u64, String>>>,
}

impl<'a> DingoStore<'a> {
    pub fn new(fname: &'a str) -> DingoStore<'a> {
        DingoStore {
            fname, 
            keys: vec![],
            objs: BTreeMap::new(),
            treesize: 0,
            flushed_files: Arc::new(Mutex::new(BTreeMap::new())),
            index: Arc::new(Mutex::new(BTreeMap::new())),
        } 
    }

    pub fn insert(&mut self, key: u64, val: String, flush: bool) -> (u64, String) {
        let new_size = self.treesize + std::mem::size_of::<u64>() as u32 + size_of_val(&val) as u32;
        
        if new_size > SIZE_THRESH  && flush{
            self.flush();
            self.objs.insert(key, val.clone());
            self.treesize = std::mem::size_of::<u64>() as u32 + size_of_val(&val) as u32;
        } else {
            if let Some(old_val) = self.objs.get(&key) {
                self.treesize -= size_of_val(old_val) as u32;
            } else {
                self.treesize += std::mem::size_of::<u64>() as u32;
            }
            self.treesize += size_of_val(&val) as u32;
            self.objs.insert(key, val.clone());
        }
        self.keys = self.objs.keys().cloned().collect();
        (key, val)
    }

    pub fn keys(&self) -> Vec<u64> {
        self.keys.clone()
    }

    fn serialize(&self, key: u64, val: &str) -> Vec<u8> {
        let mut bytes = Vec::new();
        let val_bytes = val.as_bytes();
        
        bytes.extend_from_slice(&key.to_be_bytes());
        bytes.extend_from_slice(&(val_bytes.len() as u32).to_be_bytes());
        bytes.extend_from_slice(val_bytes);
        
        bytes
    }

    pub fn get(&self, key: u64) -> Option<String> {
        // Check in-memory store first
        if let Some(val) = self.objs.get(&key) {
            return Some(val.clone());
        }

        let index = self.index.lock().unwrap();
        if let Some(data_filename) = index.get(&key) {
            let tempds = self.try_deserialize(data_filename);
            if let Ok(store) = tempds {
                if let Some(val) = store.objs.get(&key) {
                    return Some(val.clone());
                }
            }
        }
        None
    }

    fn try_deserialize(&self, filename: &str) -> Result<Self, std::io::Error> {
        let file = File::open(filename)?;
        let mut reader = BufReader::new(file);
        let mut new_store = DingoStore::new(self.fname);

        loop {
            let mut key_bytes = [0u8; 8];
            let mut val_len_bytes = [0u8; 4];
            
            if reader.read_exact(&mut key_bytes).is_err() {
                break; // End of file
            }
            if reader.read_exact(&mut val_len_bytes).is_err() {
                break; // Unexpected end of file
            }
            
            let key = u64::from_be_bytes(key_bytes);
            let val_len = u32::from_be_bytes(val_len_bytes) as usize;
            
            let mut val_bytes = vec![0u8; val_len];
            reader.read_exact(&mut val_bytes)?;
            
            let val = String::from_utf8(val_bytes).unwrap();
            
            new_store.objs.insert(key, val);
            new_store.treesize += (std::mem::size_of::<u64>() + val_len) as u32;
        }
        new_store.keys = new_store.objs.keys().cloned().collect();
        Ok(new_store)
    }

    // pub fn delete(&mut self, key: u64) -> Option<String> {
    //     if let Some(val) = self.objs.remove(&key) {
    //         self.treesize -= std::mem::size_of::<u64>() as u32;
    //         self.treesize -= size_of_val(&val) as u32;
    //         self.keys.retain(|&k| k != key);
    //         Some(val)
    //     } else {
    //         let mut index = self.index.lock().unwrap();
    //         if let Some(data_filename) = index.remove(&key) {
    //             let tempds = self.try_deserialize(&data_filename);
    //             if let Ok(mut store) = tempds {
    //                 if let Some(val) = store.objs.remove(&key) {
    //                     // Update the file without the deleted key-value pair
    //                     self.flush(&mut store, &data_filename);
    //                     Some(val)
    //                 } else {
    //                     None
    //                 }
    //             } else {
    //                 None
    //             }
    //         } else {
    //             None
    //         }
    //     }
    // }

    pub fn deserialize(filename: &'a str) -> Option<Self> {
        let mut new_store = DingoStore::new(filename);
        let deserialized_store = new_store.try_deserialize(filename);
        if let Ok(store) = deserialized_store {
            return Some(store);
        }
        None
    }

    fn flush(&mut self) {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
        let data_fname = format!("{}_{}.data", self.fname, ts);
        let mut data_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&data_fname)
            .unwrap();
        let mut index = self.index.lock().unwrap();

        for (key, val) in &self.objs {
            let bytes = self.serialize(*key, val);
            
            // Write to data file
            data_file.write_all(&bytes).unwrap();
            
            // Update index
            index.insert(*key, data_fname.clone());
        }
        
        data_file.sync_all().unwrap();
        
        let mut flushed_files = self.flushed_files.lock().unwrap();
        flushed_files.insert(data_fname.clone(), data_fname);
        self.objs.clear();
        self.treesize = 0;
        
    }
    fn compact(&mut self) {
        let mut flushed_files = self.flushed_files.lock().unwrap();
        let mut index = self.index.lock().unwrap();
        let mut masterstore = DingoStore::new(self.fname); 
        for (_, filename) in flushed_files.iter() {
            if let Ok(store) = self.try_deserialize(filename) {
                for (key, value) in store.objs {
                    masterstore.insert(key, value, false);
                }
            }
        }
        masterstore.flush();
    }
    pub fn clone(&self) -> Self {
        let mut new_store = DingoStore::new(self.fname);
        new_store.objs = self.objs.clone();
        new_store.keys = self.keys.clone();
        new_store.treesize = self.treesize;
        new_store.flushed_files = Arc::clone(&self.flushed_files);
        new_store.index = Arc::clone(&self.index);
        new_store
    }
}
