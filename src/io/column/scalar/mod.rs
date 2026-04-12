use crate::{
    core::MurrError,
    io::{
        self, codec::Decoder, codec::Encoder, column::ColumnSegmentBytes,
        directory::DirectoryReader,
    },
};

pub trait ScalarColumnReader<R: DirectoryReader, T: Decoder<T>> {}

pub trait ScalarColumnWriter<T: Encoder<T>> {
    async fn write(&self, values: &T::A) -> Result<ColumnSegmentBytes, MurrError>;
}
