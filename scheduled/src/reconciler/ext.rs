use kube::Resource;

pub trait OwnerReferenceExt {
    fn is_controlled_by<K: Resource>(&self, owner: &K) -> bool;
}

impl<S: Resource> OwnerReferenceExt for S {
    fn is_controlled_by<K: Resource>(&self, owner: &K) -> bool {
        match owner.meta().uid.as_deref() {
            Some(owner_uid) => self
                .meta()
                .owner_references
                .iter()
                .flatten()
                .any(|ref owner_reference| owner_reference.uid == owner_uid),
            None => false,
        }
    }
}
