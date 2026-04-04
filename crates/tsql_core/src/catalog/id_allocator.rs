use super::*;

impl IdAllocator for CatalogImpl {
    fn alloc_table_id(&mut self) -> u32 {
        let id = self.next_table_id;
        self.next_table_id += 1;
        id
    }

    fn alloc_column_id(&mut self) -> u32 {
        let id = self.next_column_id;
        self.next_column_id += 1;
        id
    }

    fn alloc_index_id(&mut self) -> u32 {
        let id = self.next_index_id;
        self.next_index_id += 1;
        id
    }

    fn alloc_object_id(&mut self) -> i32 {
        let id = self.next_object_id;
        self.next_object_id -= 1;
        id
    }

    fn alloc_schema_id(&mut self) -> u32 {
        let id = self.next_schema_id;
        self.next_schema_id += 1;
        id
    }
}
