"""Utilities for mapping DICOM keywords to metadata database columns."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional

from pydicom.datadict import tag_for_keyword
from pydicom.tag import Tag


Converter = Callable[[Any], Any]
FallbackGetter = Callable[[Any], Any]


def _to_str(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return str(value)
    except Exception:  # pragma: no cover - defensive guard
        return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _to_backslash_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple)) or (hasattr(value, "__iter__") and not isinstance(value, (str, bytes))):
        return "\\".join(str(item) for item in value)
    return _to_str(value)


def _to_dicom_date(value: Any) -> str | None:
    """Convert DICOM date (YYYYMMDD) to ISO format (YYYY-MM-DD) for PostgreSQL DATE column."""
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    # DICOM format: YYYYMMDD
    if len(raw) == 8 and raw.isdigit():
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
    # Already in ISO format
    if len(raw) == 10 and raw[4] == '-' and raw[7] == '-':
        return raw
    return None


def _to_dicom_time(value: Any) -> str | None:
    """Convert DICOM time (HHMMSS[.ffffff]) to PostgreSQL TIME format (HH:MM:SS[.ffffff])."""
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    
    # Handle HHMMSS.ffffff format
    if '.' in raw:
        time_part, frac_part = raw.split('.', 1)
    else:
        time_part, frac_part = raw, None
    
    # Convert HHMMSS to HH:MM:SS
    if len(time_part) >= 6 and time_part[:6].isdigit():
        formatted = f"{time_part[:2]}:{time_part[2:4]}:{time_part[4:6]}"
        if frac_part:
            formatted += f".{frac_part}"
        return formatted
    
    # Already in HH:MM:SS format
    if ':' in raw:
        return raw
    
    return None


def _to_json(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
            json_ready = []
            for item in value:
                if hasattr(item, "to_json_dict"):
                    json_ready.append(item.to_json_dict())
                else:
                    json_ready.append(str(item))
            return json.dumps(json_ready)
        if hasattr(value, "to_json_dict"):
            return json.dumps(value.to_json_dict())
        return json.dumps(value)
    except Exception:
        return None


@dataclass(frozen=True)
class FieldMapping:
    keyword: str
    converter: Converter = _to_str
    fallback: Optional[FallbackGetter] = None


def _from_file_meta(*keywords: str) -> FallbackGetter:
    def getter(dataset: Any) -> Any:
        file_meta = getattr(dataset, "file_meta", None)
        if file_meta is None:
            return None
        for key in keywords:
            if hasattr(file_meta, key):
                return getattr(file_meta, key)
        return None

    return getter


def _chain(*getters: FallbackGetter) -> FallbackGetter:
    """Chain multiple fallback getters; returns the first non-None result."""
    def getter(dataset: Any) -> Any:
        for g in getters:
            v = g(dataset)
            if v is not None:
                return v
        return None
    return getter


def _from_enhanced_fg(*paths: tuple[str, str]) -> FallbackGetter:
    """Fallback for Enhanced MR: walk SharedFunctionalGroupsSequence and
    PerFrameFunctionalGroupsSequence[0] looking for a tag.

    ``paths`` is a list of (sequence_keyword, attribute_keyword) pairs.
    Returns the first non-None value found across both FG roots and all paths.
    """
    def getter(dataset: Any) -> Any:
        for fg_root in ("SharedFunctionalGroupsSequence",
                        "PerFrameFunctionalGroupsSequence"):
            fg = getattr(dataset, fg_root, None)
            if not fg or len(fg) == 0:
                continue
            container = fg[0]
            for seq_kw, attr_kw in paths:
                seq = getattr(container, seq_kw, None)
                if seq and len(seq) > 0:
                    val = getattr(seq[0], attr_kw, None)
                    if val is not None:
                        return val
        return None
    return getter


# Private per-frame sequence tags for Enhanced MR metadata
# Philips stores full Classic-MR-style metadata in (2005,140F)
# Siemens stores ScanningSequence/SequenceVariant in (0021,1201)
_PHILIPS_PRIVATE_PER_FRAME = (0x2005, 0x140F)
_SIEMENS_PRIVATE_PER_FRAME = (0x0021, 0x1201)


def _from_enhanced_private(attr: str) -> FallbackGetter:
    """Fallback for Enhanced MR: read an attribute from manufacturer-specific
    private per-frame sequences in PerFrameFunctionalGroupsSequence[0].

    Checks Philips (2005,140F) and Siemens (0021,1201) private sub-sequences.
    """
    from pydicom.tag import Tag as _Tag

    tag_addrs = [_PHILIPS_PRIVATE_PER_FRAME, _SIEMENS_PRIVATE_PER_FRAME]

    def getter(dataset: Any) -> Any:
        fg = getattr(dataset, "PerFrameFunctionalGroupsSequence", None)
        if not fg or len(fg) == 0:
            return None
        frame0 = fg[0]
        for grp, elem in tag_addrs:
            t = _Tag(grp, elem)
            if t in frame0:
                seq_elem = frame0[t]
                if seq_elem.VR == "SQ" and seq_elem.value:
                    sub = seq_elem.value[0]
                    val = getattr(sub, attr, None)
                    if val is not None:
                        return val
        return None
    return getter


def extract_fields(dataset, mappings: dict[str, FieldMapping]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for column, mapping in mappings.items():
        raw_value = getattr(dataset, mapping.keyword, None)
        if raw_value is None and mapping.fallback is not None:
            raw_value = mapping.fallback(dataset)
        result[column] = mapping.converter(raw_value)
    return result


STUDY_FIELD_MAP: dict[str, FieldMapping] = {
    "study_date": FieldMapping("StudyDate", _to_dicom_date),
    "study_time": FieldMapping("StudyTime", _to_dicom_time),
    "study_description": FieldMapping("StudyDescription"),
    "study_comments": FieldMapping("StudyComments"),
    "modality": FieldMapping("ModalitiesInStudy", _to_backslash_str),
    "manufacturer": FieldMapping("Manufacturer"),
    "manufacturer_model_name": FieldMapping("ManufacturerModelName"),
    "station_name": FieldMapping("StationName"),
    "institution_name": FieldMapping("InstitutionName"),
}


SERIES_FIELD_MAP: dict[str, FieldMapping] = {
    "frame_of_reference_uid": FieldMapping("FrameOfReferenceUID"),
    "implementation_class_uid": FieldMapping("ImplementationClassUID", fallback=_from_file_meta("ImplementationClassUID")),
    "media_storage_sop_instance_uid": FieldMapping(
        "MediaStorageSOPInstanceUID",
        fallback=_from_file_meta("MediaStorageSOPInstanceUID"),
    ),
    "sop_class_uid": FieldMapping("SOPClassUID", fallback=_from_file_meta("MediaStorageSOPClassUID")),
    "implementation_version_name": FieldMapping(
        "ImplementationVersionName",
        fallback=_from_file_meta("ImplementationVersionName"),
    ),
    "sequence_name": FieldMapping("SequenceName"),
    "protocol_name": FieldMapping("ProtocolName"),
    "series_date": FieldMapping("SeriesDate", _to_dicom_date),
    "series_time": FieldMapping("SeriesTime", _to_dicom_time),
    "series_description": FieldMapping("SeriesDescription"),
    "body_part_examined": FieldMapping("BodyPartExamined"),
    "scanning_sequence": FieldMapping(
        "ScanningSequence",
        fallback=_from_enhanced_private("ScanningSequence"),
    ),
    "sequence_variant": FieldMapping(
        "SequenceVariant", _to_backslash_str,
        fallback=_from_enhanced_private("SequenceVariant"),
    ),
    "scan_options": FieldMapping("ScanOptions"),
    "series_comments": FieldMapping("SeriesComments"),
    "image_type": FieldMapping("ImageType", _to_backslash_str),
    "slice_thickness": FieldMapping(
        "SliceThickness", _to_float,
        fallback=_from_enhanced_fg(("PixelMeasuresSequence", "SliceThickness")),
    ),
    "spacing_between_slices": FieldMapping(
        "SpacingBetweenSlices", _to_float,
        fallback=_from_enhanced_fg(("PixelMeasuresSequence", "SpacingBetweenSlices")),
    ),
    "images_in_acquisition": FieldMapping("ImagesInAcquisition"),
    "image_orientation_patient": FieldMapping(
        "ImageOrientationPatient", _to_backslash_str,
        fallback=_from_enhanced_fg(("PlaneOrientationSequence", "ImageOrientationPatient")),
    ),
    "image_position_patient": FieldMapping("ImagePositionPatient", _to_backslash_str),
    "patient_position": FieldMapping("PatientPosition"),
    "contrast_bolus_agent": FieldMapping("ContrastBolusAgent"),
    "contrast_bolus_route": FieldMapping("ContrastBolusRoute"),
    "contrast_bolus_total_dose": FieldMapping("ContrastBolusTotalDose", _to_float),
    "contrast_bolus_start_time": FieldMapping("ContrastBolusStartTime", _to_dicom_time),
    "contrast_bolus_volume": FieldMapping("ContrastBolusVolume", _to_float),
    "contrast_flow_rate": FieldMapping("ContrastFlowRate", _to_float),
    "contrast_flow_duration": FieldMapping("ContrastFlowDuration", _to_float),
}


# Stack-defining fields - extracted for stack creation but NOT stored in instance table
# These fields are stored only in series_stack table
STACK_DEFINING_FIELDS: set[str] = {
    # MR fields
    "inversion_time",
    "echo_time",
    "echo_numbers",
    "echo_train_length",
    "repetition_time",
    "flip_angle",
    "receive_coil_name",
    "image_orientation_patient",
    "image_type",
    # CT fields
    "xray_exposure",
    "kvp",
    "tube_current",
    # PET fields
    "pet_bed_index",
    "pet_frame_type",
}


INSTANCE_FIELD_MAP: dict[str, FieldMapping] = {
    # Core instance fields (stored in instance table)
    "instance_number": FieldMapping("InstanceNumber", _to_int),
    "acquisition_number": FieldMapping("AcquisitionNumber", _to_int),
    "acquisition_date": FieldMapping("AcquisitionDate", _to_dicom_date),
    "acquisition_time": FieldMapping("AcquisitionTime", _to_dicom_time),
    "content_date": FieldMapping("ContentDate", _to_dicom_date),
    "content_time": FieldMapping("ContentTime", _to_dicom_time),
    "slice_location": FieldMapping("SliceLocation", _to_float),
    "pixel_spacing": FieldMapping(
        "PixelSpacing", _to_backslash_str,
        fallback=_from_enhanced_fg(("PixelMeasuresSequence", "PixelSpacing")),
    ),
    "rows": FieldMapping("Rows", _to_int),
    "columns": FieldMapping("Columns", _to_int),
    "bits_allocated": FieldMapping("BitsAllocated", _to_int),
    "bits_stored": FieldMapping("BitsStored", _to_int),
    "high_bit": FieldMapping("HighBit", _to_int),
    "pixel_representation": FieldMapping("PixelRepresentation", _to_int),
    "window_center": FieldMapping("WindowCenter", _to_backslash_str),
    "window_width": FieldMapping("WindowWidth", _to_backslash_str),
    "rescale_intercept": FieldMapping("RescaleIntercept", _to_float),
    "rescale_slope": FieldMapping("RescaleSlope", _to_float),
    "number_of_frames": FieldMapping("NumberOfFrames", _to_int),
    "lossy_image_compression": FieldMapping("LossyImageCompression"),
    "derivation_description": FieldMapping("DerivationDescription"),
    "image_comments": FieldMapping("ImageComments"),
    "transfer_syntax_uid": FieldMapping(
        "TransferSyntaxUID",
        _to_str,
        fallback=_from_file_meta("TransferSyntaxUID"),
    ),
    # Stack-defining fields - extracted for stack creation (stored in series_stack only)
    # MR fields — Enhanced MR stores these in functional group sequences,
    # so each has a fallback chain: standard FG first, then private per-frame.
    "inversion_time": FieldMapping("InversionTime", _to_float),
    "echo_time": FieldMapping(
        "EchoTime", _to_float,
        fallback=_chain(
            _from_enhanced_fg(("MREchoSequence", "EffectiveEchoTime")),
            _from_enhanced_private("EchoTime"),
        ),
    ),
    "echo_numbers": FieldMapping("EchoNumbers", _to_backslash_str),
    "echo_train_length": FieldMapping(
        "EchoTrainLength", _to_int,
        fallback=_chain(
            _from_enhanced_fg(("MRTimingAndRelatedParametersSequence", "EchoTrainLength")),
            _from_enhanced_private("EchoTrainLength"),
        ),
    ),
    "repetition_time": FieldMapping(
        "RepetitionTime", _to_float,
        fallback=_chain(
            _from_enhanced_fg(("MRTimingAndRelatedParametersSequence", "RepetitionTime")),
            _from_enhanced_private("RepetitionTime"),
        ),
    ),
    "flip_angle": FieldMapping(
        "FlipAngle", _to_float,
        fallback=_chain(
            _from_enhanced_fg(("MRTimingAndRelatedParametersSequence", "FlipAngle")),
            _from_enhanced_private("FlipAngle"),
        ),
    ),
    "receive_coil_name": FieldMapping(
        "ReceiveCoilName",
        fallback=_from_enhanced_fg(("MRReceiveCoilSequence", "ReceiveCoilName")),
    ),
    "image_orientation_patient": FieldMapping(
        "ImageOrientationPatient", _to_backslash_str,
        fallback=_from_enhanced_fg(("PlaneOrientationSequence", "ImageOrientationPatient")),
    ),
    "image_type": FieldMapping("ImageType", _to_backslash_str),
    # CT fields
    "xray_exposure": FieldMapping("Exposure", _to_float),
    "kvp": FieldMapping("KVP", _to_float),
    "tube_current": FieldMapping("XRayTubeCurrent", _to_float),
    # PET fields
    "pet_bed_index": FieldMapping("NumberOfSlices", _to_int),
    "pet_frame_type": FieldMapping("SeriesType", _to_backslash_str),
}


MRI_SERIES_FIELD_MAP: dict[str, FieldMapping] = {
    "mr_acquisition_type": FieldMapping("MRAcquisitionType"),
    "angio_flag": FieldMapping("AngioFlag"),
    "repetition_time": FieldMapping(
        "RepetitionTime", _to_float,
        fallback=_chain(
            _from_enhanced_fg(("MRTimingAndRelatedParametersSequence", "RepetitionTime")),
            _from_enhanced_private("RepetitionTime"),
        ),
    ),
    "echo_time": FieldMapping(
        "EchoTime", _to_float,
        fallback=_chain(
            _from_enhanced_fg(("MREchoSequence", "EffectiveEchoTime")),
            _from_enhanced_private("EchoTime"),
        ),
    ),
    "inversion_time": FieldMapping("InversionTime", _to_float),
    "inversion_times": FieldMapping("InversionTimes", _to_backslash_str),
    "flip_angle": FieldMapping(
        "FlipAngle", _to_float,
        fallback=_chain(
            _from_enhanced_fg(("MRTimingAndRelatedParametersSequence", "FlipAngle")),
            _from_enhanced_private("FlipAngle"),
        ),
    ),
    "phase_contrast": FieldMapping("PhaseContrast"),
    "number_of_averages": FieldMapping(
        "NumberOfAverages", _to_float,
        fallback=_from_enhanced_fg(("MRAveragesSequence", "NumberOfAverages")),
    ),
    "imaging_frequency": FieldMapping("ImagingFrequency", _to_float),
    "imaged_nucleus": FieldMapping("ImagedNucleus"),
    "echo_numbers": FieldMapping("EchoNumbers", _to_backslash_str),
    "magnetic_field_strength": FieldMapping("MagneticFieldStrength", _to_float),
    "number_of_phase_encoding_steps": FieldMapping("NumberOfPhaseEncodingSteps", _to_backslash_str),
    "echo_train_length": FieldMapping(
        "EchoTrainLength", _to_int,
        fallback=_chain(
            _from_enhanced_fg(("MRTimingAndRelatedParametersSequence", "EchoTrainLength")),
            _from_enhanced_private("EchoTrainLength"),
        ),
    ),
    "percent_sampling": FieldMapping(
        "PercentSampling", _to_float,
        fallback=_from_enhanced_fg(("MRFOVGeometrySequence", "PercentSampling")),
    ),
    "percent_phase_field_of_view": FieldMapping(
        "PercentPhaseFieldOfView", _to_float,
        fallback=_from_enhanced_fg(("MRFOVGeometrySequence", "PercentPhaseFieldOfView")),
    ),
    "pixel_bandwidth": FieldMapping(
        "PixelBandwidth", _to_backslash_str,
        fallback=_from_enhanced_fg(("MRImagingModifierSequence", "PixelBandwidth")),
    ),
    "receive_coil_name": FieldMapping(
        "ReceiveCoilName",
        fallback=_from_enhanced_fg(("MRReceiveCoilSequence", "ReceiveCoilName")),
    ),
    "transmit_coil_name": FieldMapping(
        "TransmitCoilName",
        fallback=_from_enhanced_fg(("MRTransmitCoilSequence", "TransmitCoilName")),
    ),
    "acquisition_matrix": FieldMapping("AcquisitionMatrix", _to_backslash_str),
    "phase_encoding_direction": FieldMapping("PhaseEncodingDirection"),
    "sar": FieldMapping("SAR", _to_float),
    "dbdt": FieldMapping("dBdt"),
    "b1rms": FieldMapping("B1rms", _to_float),
    "temporal_position_identifier": FieldMapping("TemporalPositionIdentifier", _to_int),
    "number_of_temporal_positions": FieldMapping("NumberOfTemporalPositions", _to_int),
    "temporal_resolution": FieldMapping("TemporalResolution", _to_backslash_str),
    "diffusion_b_value": FieldMapping(
        "DiffusionBValue", _to_backslash_str,
        fallback=_from_enhanced_fg(("MRDiffusionSequence", "DiffusionBValue")),
    ),
    "diffusion_gradient_orientation": FieldMapping("DiffusionGradientOrientation", _to_backslash_str),
    "diffusion_directionality": FieldMapping(
        "DiffusionDirectionality",
        fallback=_from_enhanced_fg(("MRDiffusionSequence", "DiffusionDirectionality")),
    ),
    "parallel_acquisition_technique": FieldMapping(
        "ParallelAcquisitionTechnique",
        fallback=_from_enhanced_fg(("MRModifierSequence", "ParallelAcquisitionTechnique")),
    ),
    "parallel_reduction_factor_in_plane": FieldMapping(
        "ParallelReductionFactorInPlane", _to_backslash_str,
        fallback=_from_enhanced_fg(("MRModifierSequence", "ParallelReductionFactorInPlane")),
    ),
}


CT_SERIES_FIELD_MAP: dict[str, FieldMapping] = {
    "kvp": FieldMapping("KVP", _to_float),
    "data_collection_diameter": FieldMapping("DataCollectionDiameter", _to_float),
    "reconstruction_diameter": FieldMapping("ReconstructionDiameter", _to_float),
    "gantry_detector_tilt": FieldMapping("GantryDetectorTilt", _to_float),
    "table_height": FieldMapping("TableHeight", _to_float),
    "rotation_direction": FieldMapping("RotationDirection"),
    "exposure_time": FieldMapping("ExposureTime", _to_float),
    "x_ray_tube_current": FieldMapping("XRayTubeCurrent", _to_float),
    "exposure": FieldMapping("Exposure", _to_float),
    "filter_type": FieldMapping("FilterType"),
    "generator_power": FieldMapping("GeneratorPower", _to_float),
    "focal_spots": FieldMapping("FocalSpots", _to_backslash_str),
    "convolution_kernel": FieldMapping("ConvolutionKernel"),
    "revolution_time": FieldMapping("RevolutionTime", _to_float),
    "single_collimation_width": FieldMapping("SingleCollimationWidth", _to_float),
    "total_collimation_width": FieldMapping("TotalCollimationWidth", _to_float),
    "table_speed": FieldMapping("TableSpeed", _to_float),
    "table_feed_per_rotation": FieldMapping("TableFeedPerRotation", _to_float),
    "spiral_pitch_factor": FieldMapping("SpiralPitchFactor", _to_float),
    "exposure_modulation_type": FieldMapping("ExposureModulationType"),
    "ctdi_vol": FieldMapping("CTDIvol", _to_float),
    "ctdi_phantom_type_code_sequence": FieldMapping("CTDIPhantomTypeCodeSequence", _to_json),
    "calcium_scoring_mass_factor_device": FieldMapping("CalciumScoringMassFactorDevice", _to_float),
    "calcium_scoring_mass_factor_patient": FieldMapping("CalciumScoringMassFactorPatient", _to_float),
}


PET_SERIES_FIELD_MAP: dict[str, FieldMapping] = {
    "radiopharmaceutical": FieldMapping("Radiopharmaceutical"),
    "radionuclide_total_dose": FieldMapping("RadionuclideTotalDose", _to_float),
    "radionuclide_half_life": FieldMapping("RadionuclideHalfLife", _to_float),
    "radionuclide_positron_fraction": FieldMapping("RadionuclidePositronFraction", _to_float),
    "radiopharmaceutical_start_time": FieldMapping("RadiopharmaceuticalStartTime", _to_dicom_time),
    "radiopharmaceutical_stop_time": FieldMapping("RadiopharmaceuticalStopTime", _to_dicom_time),
    "radiopharmaceutical_volume": FieldMapping("RadiopharmaceuticalVolume", _to_float),
    "radiopharmaceutical_route": FieldMapping("RadiopharmaceuticalRoute"),
    "decay_correction": FieldMapping("DecayCorrection"),
    "decay_factor": FieldMapping("DecayFactor", _to_float),
    "reconstruction_method": FieldMapping("ReconstructionMethod"),
    "scatter_correction_method": FieldMapping("ScatterCorrectionMethod"),
    "attenuation_correction_method": FieldMapping("AttenuationCorrectionMethod"),
    "randoms_correction_method": FieldMapping("RandomsCorrectionMethod"),
    "dose_calibration_factor": FieldMapping("DoseCalibrationFactor", _to_float),
    "activity_concentration_scale": FieldMapping("ActivityConcentrationScale", _to_float),
    "suv_type": FieldMapping("SUVType"),
    "suvbw": FieldMapping("SUVbw", _to_float),
    "suvlbm": FieldMapping("SUVlbm", _to_float),
    "suvbsa": FieldMapping("SUVbsa", _to_float),
    "counts_source": FieldMapping("CountsSource"),
    "units": FieldMapping("Units"),
    "frame_reference_time": FieldMapping("FrameReferenceTime", _to_float),
    "actual_frame_duration": FieldMapping("ActualFrameDuration", _to_float),
    "patient_gantry_relationship_code": FieldMapping("PatientGantryRelationshipCodeSequence", _to_json),
    "slice_progression_direction": FieldMapping("SliceProgressionDirection"),
    "series_type": FieldMapping("SeriesType", _to_backslash_str),
    "units_type": FieldMapping("Units", _to_str),
    "counts_included": FieldMapping("CountsIncluded", _to_backslash_str),
}


# Core routing tags required for every DICOM file
CORE_KEYWORDS: set[str] = {
    "StudyInstanceUID",
    "SeriesInstanceUID",
    "SOPInstanceUID",
    "SOPClassUID",
    "Modality",
    "PatientID",
    "PatientName",
}


def _keywords_to_tags(keywords: set[str]) -> tuple[Tag, ...]:
    """Convert DICOM keywords to Tag tuples for use with pydicom's specific_tags parameter."""
    tags: list[Tag] = []
    for kw in sorted(keywords):
        tag_val = tag_for_keyword(kw)
        if tag_val is None:
            # Defensive: some keywords might not map cleanly; we just skip them.
            continue
        tags.append(Tag(tag_val))
    return tuple(tags)


# Build the complete set of required keywords from all field maps
EXTRACT_REQUIRED_KEYWORDS: set[str] = (
    CORE_KEYWORDS
    | {m.keyword for m in STUDY_FIELD_MAP.values()}
    | {m.keyword for m in SERIES_FIELD_MAP.values()}
    | {m.keyword for m in INSTANCE_FIELD_MAP.values()}
    | {m.keyword for m in MRI_SERIES_FIELD_MAP.values()}
    | {m.keyword for m in CT_SERIES_FIELD_MAP.values()}
    | {m.keyword for m in PET_SERIES_FIELD_MAP.values()}
)

# DWI private tag addresses (manufacturer-specific, cannot be fetched by keyword).
DWI_PRIVATE_TAGS: tuple[Tag, ...] = (
    Tag(0x0019, 0x100C),  # Siemens B_value
    Tag(0x0019, 0x100D),  # Siemens DiffusionDirectionality
    Tag(0x0029, 0x1010),  # Siemens CSA image header (PhaseEncodingDirectionPositive)
    Tag(0x0043, 0x1039),  # GE b-value array
    Tag(0x0043, 0x1030),  # GE number of diffusion directions
    Tag(0x2001, 0x1003),  # Philips DiffusionBValueNumber
)

# Tag list kept for reference / non-NFS use cases.
# In the main extract path (process_pool + worker) we do NOT pass specific_tags
# to pydicom. Benchmarks show that stop_before_pixels=True without specific_tags
# is ~10% faster on NFS than with a specific_tags filter, because pydicom's
# native early-exit doesn't need to maintain a "remaining tags" checklist.
# It also gives us private tags (incl. CSA at 0029,1010) for free with no
# reliability risk from fixed-size partial reads.
EXTRACT_SPECIFIC_TAGS: tuple[Tag, ...] = (
    _keywords_to_tags(EXTRACT_REQUIRED_KEYWORDS) + DWI_PRIVATE_TAGS
)


# ---------------------------------------------------------------------------
# DWI private-tag getters
# ---------------------------------------------------------------------------
# These functions read manufacturer-specific private DICOM tags that cannot
# be accessed via pydicom keyword attributes. Each returns None on missing /
# malformed data rather than raising.

def _parse_siemens_csa_pe_dir_positive(dataset: Any) -> int | None:
    """Parse Siemens CSA SV10 header to extract PhaseEncodingDirectionPositive.

    The CSA header at (0029,1010) is a proprietary binary format. This parser
    implements the SV10 variant only (magic bytes b'SV10'). Returns 1 (positive),
    0 (negative), or None if the field is absent or the format is not recognised.
    """
    import struct

    csa_tag = Tag(0x0029, 0x1010)
    elem = dataset.get(csa_tag)
    if elem is None:
        return None
    try:
        data = bytes(elem.value)
    except Exception:
        return None
    if len(data) < 8 or not data.startswith(b"SV10"):
        return None
    try:
        pos = 8
        n_tags = struct.unpack_from("<I", data, pos)[0]
        pos += 8  # skip n_tags (4 bytes) + unused (4 bytes)
        target = b"PhaseEncodingDirectionPositive"
        for _ in range(n_tags):
            if pos + 84 > len(data):
                break
            name_bytes = data[pos : pos + 64]
            # Split at first null byte — Siemens appends frequency-table bytes after the null
            null_pos = name_bytes.find(b"\x00")
            name = name_bytes[:null_pos] if null_pos >= 0 else name_bytes
            pos += 64
            pos += 12  # skip vm (4), vr (4), syngo_dt (4)
            n_items = struct.unpack_from("<I", data, pos)[0]
            pos += 4
            pos += 4  # unused
            values: list[str] = []
            for _ in range(n_items):
                if pos + 16 > len(data):
                    break
                length = struct.unpack_from("<I", data, pos)[0]
                pos += 4
                pos += 12  # unused
                if length > 0:
                    aligned = (length + 3) & ~3
                    raw_val = data[pos : pos + length].rstrip(b"\x00").decode("latin-1", errors="replace").strip()
                    values.append(raw_val)
                    pos += aligned
            if name == target and values:
                try:
                    return int(values[0])
                except (ValueError, TypeError):
                    return None
    except Exception:
        return None
    return None


def get_dwi_siemens_b_value(dataset: Any) -> int | None:
    """Read Siemens private b-value from (0019,100C)."""
    elem = dataset.get(Tag(0x0019, 0x100C))
    if elem is None:
        return None
    try:
        return int(str(elem.value).strip())
    except (ValueError, TypeError):
        return None


def get_dwi_siemens_directionality(dataset: Any) -> str | None:
    """Read Siemens DiffusionDirectionality from (0019,100D): 'DIRECTIONAL' or 'NONE'."""
    elem = dataset.get(Tag(0x0019, 0x100D))
    if elem is None:
        return None
    return str(elem.value).strip() or None


def get_dwi_siemens_pe_dir_positive(dataset: Any) -> int | None:
    """Extract PhaseEncodingDirectionPositive from Siemens CSA header (0029,1010)."""
    return _parse_siemens_csa_pe_dir_positive(dataset)


def get_dwi_ge_b_value(dataset: Any) -> int | None:
    """Read GE private b-value from (0043,1039): a 4-element IS array; first element is b-value."""
    elem = dataset.get(Tag(0x0043, 0x1039))
    if elem is None:
        return None
    try:
        val = elem.value
        first = val[0] if isinstance(val, (list, tuple)) else val
        return int(str(first).strip())
    except (ValueError, TypeError, IndexError):
        return None


def get_dwi_ge_n_directions(dataset: Any) -> int | None:
    """Read GE number of diffusion directions from (0043,1030)."""
    elem = dataset.get(Tag(0x0043, 0x1030))
    if elem is None:
        return None
    try:
        return int(elem.value)
    except (ValueError, TypeError):
        return None


def get_dwi_philips_b_value(dataset: Any) -> float | None:
    """Read Philips private DiffusionBValueNumber from (2001,1003).

    Filters the Philips sentinel value (~1.7e+38) used for non-DW volumes.
    """
    _PHILIPS_SENTINEL = 1e37
    elem = dataset.get(Tag(0x2001, 0x1003))
    if elem is None:
        return None
    try:
        v = float(elem.value)
        return None if v > _PHILIPS_SENTINEL else v
    except (ValueError, TypeError):
        return None
