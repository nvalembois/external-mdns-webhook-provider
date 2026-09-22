use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use simple_dns::QTYPE;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum RecordType {
    A,
    AAAA,
    CNAME,
    TXT,
    SRV,
    NS,
    PTR,
    MX,
    NAPTR
}

impl PartialEq<QTYPE> for RecordType {
	fn eq(&self, other: &QTYPE) -> bool {
		match other {
			QTYPE::TYPE(simple_dns::TYPE::A) => *self == RecordType::A,
			QTYPE::TYPE(simple_dns::TYPE::AAAA) => *self == RecordType::AAAA,
			QTYPE::TYPE(simple_dns::TYPE::CNAME) => *self == RecordType::CNAME,
			QTYPE::TYPE(simple_dns::TYPE::TXT) => *self == RecordType::TXT,
			QTYPE::TYPE(simple_dns::TYPE::SRV) => *self == RecordType::SRV,
			QTYPE::TYPE(simple_dns::TYPE::NS) => *self == RecordType::NS,
			QTYPE::TYPE(simple_dns::TYPE::PTR) => *self == RecordType::PTR,
			QTYPE::TYPE(simple_dns::TYPE::MX) => *self == RecordType::MX,
			QTYPE::TYPE(simple_dns::TYPE::NAPTR) => *self == RecordType::NAPTR,
			QTYPE::TYPE(_) => false,
			_ => false,
		}
	}
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProviderSpecificProperty {
    pub name: String,
	pub value: String,
}

pub type TTL = i64;
pub type ProviderSpecific = Vec<ProviderSpecificProperty>;
pub type Targets = Vec<String>;
pub type Labels = HashMap<String,String>;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Endpoint {
	// The hostname of the DNS record
	pub dns_name: String,
	// The targets the DNS record points to
    pub targets: Targets,
	// RecordType type of record, e.g. CNAME, A, AAAA, SRV, TXT etc
	pub record_type: RecordType,
	// Identifier to distinguish multiple records with the same name and type (e.g. Route53 records with routing policies other than 'simple')
	pub set_identifier: Option<String>,
	// TTL for the record
	pub record_t_t_l: Option<TTL>,
	// Labels stores labels defined for the Endpoint
	pub labels: Option<Labels>,
	// ProviderSpecific stores provider specific config
	pub provider_specific: Option<ProviderSpecific>,
}

impl PartialEq for Endpoint {
	fn eq(&self, other: &Self) -> bool {
		self.dns_name == other.dns_name && 
		self.targets == other.targets && 
		self.record_type == other.record_type
	}
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct Changes {
	// Records that need to be created
	pub create: Option<Vec<Endpoint>>,
	// Records that need to be updated (current data)
	pub update_old: Option<Vec<Endpoint>>,
	// Records that need to be updated (desired data)
	pub update_new: Option<Vec<Endpoint>>,
	// Records that need to be deleted
	pub delete: Option<Vec<Endpoint>>,
}
