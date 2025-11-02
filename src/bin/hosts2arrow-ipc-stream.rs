use std::io;

use arrow::ipc::writer::StreamWriter;
use hostfile::parse_hostfile;
use rs_hosts2arrow_ipc_stream::hosts2batch;

fn main() -> io::Result<()> {
    let hosts = parse_hostfile().map_err(io::Error::other)?;
    let batch = hosts2batch(hosts)?;

    let stdout = io::stdout();
    let mut writer = StreamWriter::try_new(stdout, &batch.schema())
        .map_err(|e| io::Error::other(e.to_string()))?;
    writer
        .write(&batch)
        .map_err(|e| io::Error::other(e.to_string()))?;
    writer
        .finish()
        .map_err(|e| io::Error::other(e.to_string()))?;

    Ok(())
}
