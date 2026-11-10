pub mod dtfs {
    pub mod common {
        tonic::include_proto!("dtfs.common");
    }
    pub mod data {
        tonic::include_proto!("dtfs.data");
    }
    pub mod manager {
        tonic::include_proto!("dtfs.manager");
    }
}
