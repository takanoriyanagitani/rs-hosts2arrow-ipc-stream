use std::io;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_schema::{DataType, Field, Schema};
use hostfile::HostEntry;

pub fn hosts2batch(hosts: Vec<HostEntry>) -> Result<RecordBatch, io::Error> {
    let mut ip_builder = StringBuilder::new();
    let mut names_builder = ListBuilder::new(StringBuilder::new());

    for host in hosts {
        ip_builder.append_value(host.ip.to_string());
        let names = host.names.iter().map(|s| Some(s.as_str()));
        names_builder.append_value(names);
    }

    let ip_array = ip_builder.finish();
    let names_array = names_builder.finish();

    let schema = Schema::new(vec![
        Field::new("ip", DataType::Utf8, false),
        Field::new(
            "names",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(ip_array), Arc::new(names_array)],
    )
    .map_err(|e| io::Error::other(e.to_string()))?;

    Ok(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_hosts2batch() {
        let host_entries = vec![
            HostEntry {
                ip: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                names: vec!["localhost".to_string(), "loopback".to_string()],
            },
            HostEntry {
                ip: IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)),
                names: vec!["myhost".to_string()],
            },
        ];

        let batch = hosts2batch(host_entries).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

        let ip_array = batch.column(0).as_string::<i32>();
        let names_array = batch.column(1).as_list::<i32>();

        assert_eq!(ip_array.value(0), "127.0.0.1");
        assert_eq!(ip_array.value(1), "192.168.1.100");

        let names_0 = names_array.value(0);
        assert_eq!(names_0.as_string::<i32>().value(0), "localhost");
        assert_eq!(names_0.as_string::<i32>().value(1), "loopback");

        let names_1 = names_array.value(1);
        assert_eq!(names_1.as_string::<i32>().value(0), "myhost");
    }
}
