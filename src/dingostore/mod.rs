use std::{collections::BTreeMap, fmt::{Debug, Display, Formatter}, time::{SystemTime, UNIX_EPOCH}};
use std::mem::size_of_val;
use std::fs::{File, OpenOptions};
use std::io::{Write, BufReader, Read};
use std::sync::{Arc, Mutex};

const SIZE_THRESH: u32 = 80000;
const COMPACT_LIM: usize = 10;
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
    flushed_files: Arc<Mutex<BTreeMap<u64, String>>>,
}

impl<'a> DingoStore<'a> {
    pub fn new(fname: &'a str) -> DingoStore<'a> {
        DingoStore {
            fname, 
            keys: vec![],
            objs: BTreeMap::new(),
            treesize: 0,
            flushed_files: Arc::new(Mutex::new(BTreeMap::new())),
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
        let flushed_files = self.flushed_files.lock().unwrap();
        let mut idx = 0;
        let keys = flushed_files.keys().collect::<Vec<&u64>>();
        while idx < keys.len() && keys[idx] <= &key {
            idx+=1;
        }
        let tempds = self.try_deserialize(flushed_files.get(&(keys[idx-1])).unwrap()).unwrap();
        if let Some(val) = tempds.objs.get(&key) {
            return Some(val.clone());
        }else{
            println!("miss...");
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


    pub fn deserialize(filename: &'a str) -> Option<Self> {
        let mut new_store = DingoStore::new(filename);
        let deserialized_store = new_store.try_deserialize(filename);
        if let Ok(store) = deserialized_store {
            return Some(store);
        }
        None
    }

    fn flush(&mut self) -> String {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
        let data_fname = format!("{}_{}.data", self.fname, ts);
        let mut data_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&data_fname)
            .unwrap();
        let mut firstkey : Option<u64> = None;
        for (key, val) in &self.objs {
            if firstkey.is_none() {
                firstkey = Some(*key);
            }
            let bytes = self.serialize(*key, val);
            
            // Write to data file
            data_file.write_all(&bytes).unwrap();
            
            // Update index
        }
        
        data_file.sync_all().unwrap();
        
        let mut flushed_files = self.flushed_files.lock().unwrap();
        // need to look at this...
        flushed_files.insert(firstkey.unwrap(), data_fname.clone());
        self.objs.clear();
        self.treesize = 0;
        data_fname

    }
    fn compact(&mut self) {
        
    }
    pub fn clone(&self) -> Self {
        let mut new_store = DingoStore::new(self.fname);
        new_store.objs = self.objs.clone();
        new_store.keys = self.keys.clone();
        new_store.treesize = self.treesize;
        new_store.flushed_files = Arc::clone(&self.flushed_files);
        new_store
    }
}
