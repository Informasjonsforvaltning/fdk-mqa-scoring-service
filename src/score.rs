
#[derive(Debug)]
pub struct Score {
    pub Findability: FindabilityScore,
    pub Accessibility: AccessibilityScore,
    pub Interoperability: InteroperabilityScore,
    pub Reusability: ReusabilityScore,
    pub Contextuality: ContextualityScore,
}

impl Score {
    pub fn score(&self) -> i64 {
        0
    }
}

#[derive(Debug, new)]
pub struct FindabilityScore {
    #[new(default)] 
    pub KeywordAvailability: u8,
    #[new(default)] 
    pub CategoryAvailability: u8,
    #[new(default)] 
    pub SpatialAvailability: u8,
    #[new(default)] 
    pub TemporalAvailability: u8,
}

#[derive(Debug, new)]
pub struct AccessibilityScore {
    #[new(default)] 
    pub AccessUrlStatusCode: u8,
    #[new(default)] 
    pub DownloadUrlAvailability: u8,
    #[new(default)] 
    pub DownloadUrlStatusCode: u8,
}

#[derive(Debug, new)]
pub struct InteroperabilityScore {
    #[new(default)] 
    pub FormatAvailability: u8,
    #[new(default)] 
    pub MediaTypeAvailability: u8,
    #[new(default)] 
    pub FormatMediaTypeVocabularyAlignment: u8,
    #[new(default)] 
    pub FormatMediaTypeNonProprietary: u8,
    #[new(default)] 
    pub FormatMediaTypeMachineInterpretable: u8,
    #[new(default)] 
    pub DcatApCompliance: u8,
    #[new(default)] 
    pub FormatMatch: u8,
    #[new(default)] 
    pub SyntaxValid: u8,
}

#[derive(Debug, new)]
pub struct ReusabilityScore {
    #[new(default)] 
    pub LicenceAvailability: u8,
    #[new(default)] 
    pub KnownLicence: u8,
    #[new(default)] 
    pub AccessRightsAvailability: u8,
    #[new(default)] 
    pub AccessRightsVocabularyAlignment: u8,
    #[new(default)] 
    pub ContactPointAvailability: u8,
    #[new(default)] 
    pub PublisherAvailability: u8,
}

#[derive(Debug, new)]
pub struct ContextualityScore{
    #[new(default)] 
    pub RightsAvailability: u8,
    #[new(default)] 
    pub ByteSizeAvailability: u8,
    #[new(default)] 
    pub DateIssuedAvailability: u8,
    #[new(default)] 
    pub DateModifiedAvailability: u8,
}
