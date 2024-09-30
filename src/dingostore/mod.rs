use std::{collections::BTreeMap, time::{SystemTime, UNIX_EPOCH}};
use std::mem::size_of_val;
use std::fs::OpenOptions;
use std::io::{Write, Read};
use std::sync::{Arc, Mutex};

const SIZE_THRESH: u32 = 80000;


pub struct DingoStore<'a> {
    objs: BTreeMap<u64, String>,
    fname: &'a str,
    treesize: u32,
    flushed_files: Arc<Mutex<BTreeMap<u64, String>>>,
}

impl<'a> DingoStore<'a> {
    pub fn new(fname: &'a str) -> DingoStore<'a> {
        DingoStore {
            fname, 
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
        (key, val)
    }


    fn serialize(&self, key: u64, val: &str) -> Vec<u8> {
        let mut bytes = Vec::new();
        let val_bytes = val.as_bytes();
        
        bytes.extend_from_slice(&key.to_be_bytes());
        bytes.extend_from_slice(&(val_bytes.len() as u32).to_be_bytes());
        bytes.extend_from_slice(val_bytes);
        
        bytes
    }
    fn seek_key(&self, filename: &String, key: u64) -> Option<Vec<u8>>{
        let mut f = std::io::BufReader::new(std::fs::File::open(filename).unwrap()); 
        let mut tempbuffer = [0u8; 8];
        loop {
            match f.read_exact(&mut tempbuffer) {
                Err(_) => break,
                Ok(_) => {
                    let keyparse = u64::from_be_bytes(tempbuffer);
                    let mut value_len_buffer = [0u8; 4];
                    f.read_exact(&mut value_len_buffer); 
                    let valuelen = u32::from_be_bytes(value_len_buffer);
                    let mut valbuff = vec![0u8; valuelen as usize];
                    f.read_exact(&mut valbuff).unwrap();
                    if keyparse == key {
                        return Some(valbuff);
                    }

                }
            }

        } 

        return None;
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
        let target_filename = flushed_files.get(&(keys[idx-1])).unwrap();
        let find_res = self.seek_key(target_filename, key);
        match find_res {
            Some(v) => {

                let value = String::from_utf8_lossy(&v);
                return Some(value.to_string());
            },
            None => {
                return None;
            }
        }
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
        flushed_files.insert(firstkey.unwrap(), data_fname.clone());
        self.objs.clear();
        self.treesize = 0;
        data_fname

    }
    
}
