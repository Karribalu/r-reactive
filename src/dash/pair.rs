pub(crate) type ValueT = Vec<u8>;

#[derive(Debug, Clone, PartialEq)]
pub struct Pair<T: PartialEq + Clone> {
    pub key: T,
    pub value: ValueT,
}

impl<T: PartialEq + Clone> Pair<T> {
    pub fn new(key: T, value: ValueT) -> Self {
        Pair { key, value }
    }
}
