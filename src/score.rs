
extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn;

#[proc_macro_derive(HelloMacro)]
pub fn hello_macro_derive(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let ast = syn::parse(input).unwrap();

    // Build the trait implementation
    impl_hello_macro(&ast)
}

fn impl_hello_macro(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let gen = quote! {
        impl HelloMacro for #name {
            fn hello_macro() {
                println!("Hello, Macro! My name is {}!", stringify!(#name));
            }
        }
    };
    gen.into()
}
#[derive(Debug, Default)]
pub struct Score {
    pub findability: FindabilityScore,
    pub accessibility: AccessibilityScore,
    pub interoperability: InteroperabilityScore,
    pub reusability: ReusabilityScore,
    pub contextuality: ContextualityScore,
}

#[derive(Debug, Default, Score)]
pub struct FindabilityScore {
    #[metric_true(KEYWORD_AVAILABILITY 25)]
    pub keyword_availability: u8,
    #[metric_true(CATEGORY_AVAILABILITY, 25)]
    pub category_availability: u8,
    #[metric_true(SPATIAL_AVAILABILITY, 25)]
    pub spatial_availability: u8,
    #[metric_true(TEMPORAL_AVAILABILITY, 25)]
    pub temporal_availability: u8,
}

// generated
impl FindabilityScore {
    fn score() -> u64 {
        keyword_availability + category_availability + spatial_availability + temporal_availability
    }

    fn fetch_scores(&store) {
        keyword_availability = ...
        category_availability = ...
    } 
}

#[derive(Debug, Default, PartialEq)]
pub struct AccessibilityScore {
    #[int_in_range(metric_name, 200, 299, 50)]
    pub access_url_status_code: u8,
    #[int_in_range(metric_name, 200, 299, 20)]
    pub download_url_availability: u8,
    #[int_in_range(metric_name, 200, 299, 30)]
    pub download_url_status_code: u8,
}

#[derive(Debug, Default)]
pub struct InteroperabilityScore {
    pub format_availability: u8,
    pub media_type_availability: u8,
    pub format_media_type_vocabulary_alignment: u8,
    pub format_media_type_non_proprietary: u8,
    pub format_media_type_machine_interpretable: u8,
    pub dcat_ap_compliance: u8,
    pub format_match: u8,
    pub syntax_valid: u8,
}

#[derive(Debug, Default)]
pub struct ReusabilityScore {
    pub licence_availability: u8,
    pub known_licence: u8,
    pub access_rights_availability: u8,
    pub access_rights_vocabulary_alignment: u8,
    pub contact_point_availability: u8,
    pub publisher_availability: u8,
}

#[derive(Debug, Default)]
pub struct ContextualityScore {
    pub rights_availability: u8,
    pub byte_size_availability: u8,
    pub date_issued_availability: u8,
    pub date_modified_availability: u8,
}

impl Score {
    pub fn score(&self) -> u32 {
        vec![
            self.findability.score(),
            self.accessibility.score(),
            self.interoperability.score(),
            self.reusability.score(),
            self.contextuality.score(),
        ]
        .iter()
        .sum()
    }
}

impl FindabilityScore {
    pub fn score(&self) -> u32 {
        vec![
            self.keyword_availability,
            self.category_availability,
            self.spatial_availability,
            self.temporal_availability,
        ]
        .iter()
        .map(|&i| i as u32)
        .sum()
    }
}

impl AccessibilityScore {
    pub fn score(&self) -> u32 {
        vec![
            self.access_url_status_code,
            self.download_url_availability,
            self.download_url_status_code,
        ]
        .iter()
        .map(|&i| i as u32)
        .sum()
    }
}

impl InteroperabilityScore {
    pub fn score(&self) -> u32 {
        vec![
            self.format_availability,
            self.media_type_availability,
            self.format_media_type_vocabulary_alignment,
            self.format_media_type_non_proprietary,
            self.format_media_type_machine_interpretable,
            self.dcat_ap_compliance,
            self.format_match,
            self.syntax_valid,
        ]
        .iter()
        .map(|&i| i as u32)
        .sum()
    }
}

impl ReusabilityScore {
    pub fn score(&self) -> u32 {
        vec![
            self.licence_availability,
            self.known_licence,
            self.access_rights_availability,
            self.access_rights_vocabulary_alignment,
            self.contact_point_availability,
            self.publisher_availability,
        ]
        .iter()
        .map(|&i| i as u32)
        .sum()
    }
}

impl ContextualityScore {
    pub fn score(&self) -> u32 {
        vec![
            self.rights_availability,
            self.byte_size_availability,
            self.date_issued_availability,
            self.date_modified_availability,
        ]
        .iter()
        .map(|&i| i as u32)
        .sum()
    }
}
