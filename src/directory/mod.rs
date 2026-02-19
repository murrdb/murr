mod local;

pub use local::LocalDirectory;

use crate::segment::Segment;

pub trait Directory {
    fn segments(&self) -> &[Segment];
}
