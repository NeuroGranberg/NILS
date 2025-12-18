"""
Tests for AccelerationDetector.

Tests detection of k-space acceleration methods:
- ParallelImaging (GRAPPA/SENSE/ARC)
- SimultaneousMultiSlice (SMS/Multiband)
- PartialFourier (PFP/PFF)
- CompressedSensing
- ViewSharing (TWIST/TRICKS)
"""

import pytest
from src.classification.core.context import ClassificationContext
from src.classification.detectors.acceleration import (
    AccelerationDetector,
    AccelerationResult,
    AccelerationDetectorOutput,
)


@pytest.fixture
def detector():
    """Create detector instance for tests."""
    return AccelerationDetector()


# =============================================================================
# OUTPUT STRUCTURE TESTS
# =============================================================================

class TestOutputStructure:
    """Test the output data structures."""
    
    def test_empty_output(self, detector):
        """Empty context produces empty acceleration list."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="brain imaging",
        )
        result = detector.detect(ctx)
        assert isinstance(result, AccelerationDetectorOutput)
        assert result.accelerations == []
        assert result.values == []
        assert result.has_acceleration is False
    
    def test_single_acceleration(self, detector):
        """Single acceleration detected."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scan_options="['ACC_GEMS', 'FS']",
            text_search_blob="t1 brain",
        )
        result = detector.detect(ctx)
        assert result.has_acceleration is True
        assert len(result.accelerations) == 1
        assert result.has("ParallelImaging")
    
    def test_multiple_accelerations(self, detector):
        """Multiple accelerations detected simultaneously."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scan_options="['ACC_GEMS', 'PFP']",
            text_search_blob="grappa with partial fourier",
        )
        result = detector.detect(ctx)
        assert result.has_acceleration is True
        assert len(result.accelerations) >= 2
        assert result.has("ParallelImaging")
        assert result.has("PartialFourier")


# =============================================================================
# PARALLEL IMAGING TESTS
# =============================================================================

class TestParallelImaging:
    """Test Parallel Imaging detection."""
    
    def test_unified_flag(self, detector):
        """Parallel imaging from unified_flags."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scan_options="['ACC_GEMS']",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("ParallelImaging")
        accel = result.get("ParallelImaging")
        assert accel.confidence >= 0.90
    
    def test_hypersense(self, detector):
        """HyperSense triggers parallel imaging."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scan_options="['HYPERSENSE_GEMS']",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("ParallelImaging")
    
    def test_grappa_keyword(self, detector):
        """GRAPPA keyword detection."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="t2w tse grappa 2 brain",
        )
        result = detector.detect(ctx)
        assert result.has("ParallelImaging")
    
    def test_sense_keyword(self, detector):
        """SENSE keyword detection."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="sense factor 2 brain",
        )
        result = detector.detect(ctx)
        assert result.has("ParallelImaging")
    
    def test_asset_keyword(self, detector):
        """ASSET keyword detection."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="t1 fspgr asset brain",
        )
        result = detector.detect(ctx)
        assert result.has("ParallelImaging")
    
    def test_arc_bounded(self, detector):
        """ARC with word boundaries."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="t1 bravo arc brain",
        )
        result = detector.detect(ctx)
        assert result.has("ParallelImaging")
    
    def test_arc_with_factor(self, detector):
        """ARC with factor notation."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="bravo arc[2x1] brain",
        )
        result = detector.detect(ctx)
        assert result.has("ParallelImaging")
    
    def test_ipat_keyword(self, detector):
        """iPAT keyword detection."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="mprage ipat2 brain",
        )
        result = detector.detect(ctx)
        assert result.has("ParallelImaging")
    
    def test_no_false_positive_search(self, detector):
        """'search' should not trigger 'arc' detection."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="brain search protocol",
        )
        result = detector.detect(ctx)
        assert not result.has("ParallelImaging")


# =============================================================================
# SIMULTANEOUS MULTI-SLICE (SMS) TESTS
# =============================================================================

class TestSimultaneousMultiSlice:
    """Test SMS/Multiband detection."""
    
    def test_multiband_keyword(self, detector):
        """Multiband keyword detection."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="dwi multiband factor 4",
        )
        result = detector.detect(ctx)
        assert result.has("SimultaneousMultiSlice")
    
    def test_mb_with_number(self, detector):
        """MB with factor number."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="dwi mb4 64dir",
        )
        result = detector.detect(ctx)
        assert result.has("SimultaneousMultiSlice")
    
    def test_mb_bracketed(self, detector):
        """MB with brackets."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="ep2d_diff mb[3] b1000",
        )
        result = detector.detect(ctx)
        assert result.has("SimultaneousMultiSlice")
    
    def test_sms_keyword(self, detector):
        """SMS keyword detection."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="bold sms epi brain",
        )
        result = detector.detect(ctx)
        assert result.has("SimultaneousMultiSlice")
    
    def test_hyperband(self, detector):
        """HyperBand detection."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            stack_sequence_name="hyperband_dwi",
            text_search_blob="dwi brain",
        )
        result = detector.detect(ctx)
        assert result.has("SimultaneousMultiSlice")
    
    def test_cmrr_sequence(self, detector):
        """CMRR sequence pattern."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            stack_sequence_name="cmrr_bold",
            text_search_blob="fmri brain",
        )
        result = detector.detect(ctx)
        assert result.has("SimultaneousMultiSlice")
    
    def test_no_false_positive_combat(self, detector):
        """'combat' should not trigger MB detection."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="combat protocol brain",
        )
        result = detector.detect(ctx)
        assert not result.has("SimultaneousMultiSlice")
    
    def test_no_false_positive_number(self, detector):
        """'number' should not trigger MB detection."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="sequence number 5 brain",
        )
        result = detector.detect(ctx)
        assert not result.has("SimultaneousMultiSlice")


# =============================================================================
# PARTIAL FOURIER TESTS
# =============================================================================

class TestPartialFourier:
    """Test Partial Fourier detection."""
    
    def test_unified_flag(self, detector):
        """Partial Fourier from unified_flags."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scan_options="['PFP']",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("PartialFourier")
        accel = result.get("PartialFourier")
        assert accel.confidence >= 0.90
    
    def test_pfp_only(self, detector):
        """Phase direction only."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scan_options="['PFP']",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("PartialFourier")
        accel = result.get("PartialFourier")
        assert "phase" in (accel.subtype or "")
    
    def test_pff_only(self, detector):
        """Frequency direction only."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scan_options="['PFF']",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("PartialFourier")
        accel = result.get("PartialFourier")
        assert "frequency" in (accel.subtype or "")
    
    def test_both_directions(self, detector):
        """Both phase and frequency."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scan_options="['PFP', 'PFF']",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("PartialFourier")
        accel = result.get("PartialFourier")
        assert "phase" in (accel.subtype or "")
        assert "frequency" in (accel.subtype or "")
    
    def test_partial_fourier_keyword(self, detector):
        """Partial Fourier keyword."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="haste partial fourier brain",
        )
        result = detector.detect(ctx)
        assert result.has("PartialFourier")
    
    def test_half_fourier_keyword(self, detector):
        """Half Fourier keyword."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="half fourier ssfse brain",
        )
        result = detector.detect(ctx)
        assert result.has("PartialFourier")
    
    def test_fraction_notation(self, detector):
        """Fraction notation (6/8, 7/8)."""
        for fraction in ["5/8", "6/8", "7/8"]:
            ctx = ClassificationContext(
                image_type="ORIGINAL\\PRIMARY\\M\\ND",
                text_search_blob=f"tse {fraction} brain",
            )
            result = detector.detect(ctx)
            assert result.has("PartialFourier"), f"Failed for {fraction}"


# =============================================================================
# COMPRESSED SENSING TESTS
# =============================================================================

class TestCompressedSensing:
    """Test Compressed Sensing detection."""
    
    def test_cs_gems_flag(self, detector):
        """CS from GE CS_GEMS flag."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scan_options="['CS_GEMS']",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("CompressedSensing")
        accel = result.get("CompressedSensing")
        assert accel.confidence >= 0.85
    
    def test_compressed_sensing_keyword(self, detector):
        """Compressed sensing full keyword."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="t1 compressed sensing brain",
        )
        result = detector.detect(ctx)
        assert result.has("CompressedSensing")
    
    def test_sparse_keyword(self, detector):
        """Sparse keyword."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="sparse mri reconstruction brain",
        )
        result = detector.detect(ctx)
        assert result.has("CompressedSensing")
    
    def test_wave_caipi(self, detector):
        """Wave-CAIPI detection."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="mprage wave-caipi brain",
        )
        result = detector.detect(ctx)
        assert result.has("CompressedSensing")
    
    def test_caipi_keyword(self, detector):
        """CAIPI keyword."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="caipi acceleration brain",
        )
        result = detector.detect(ctx)
        assert result.has("CompressedSensing")
    
    def test_no_false_positive_csf(self, detector):
        """CSF should not trigger CS detection."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="csf flow study brain",
        )
        result = detector.detect(ctx)
        assert not result.has("CompressedSensing")


# =============================================================================
# VIEW SHARING TESTS
# =============================================================================

class TestViewSharing:
    """Test View Sharing detection."""
    
    def test_twist_keyword(self, detector):
        """TWIST keyword."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="ce mra twist brain",
        )
        result = detector.detect(ctx)
        assert result.has("ViewSharing")
    
    def test_tricks_keyword(self, detector):
        """TRICKS keyword."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="mra tricks time resolved",
        )
        result = detector.detect(ctx)
        assert result.has("ViewSharing")
    
    def test_keyhole_keyword(self, detector):
        """Keyhole keyword."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="dynamic keyhole mri",
        )
        result = detector.detect(ctx)
        assert result.has("ViewSharing")
    
    def test_disco_keyword(self, detector):
        """DISCO keyword."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="dce disco liver",
        )
        result = detector.detect(ctx)
        assert result.has("ViewSharing")
    
    def test_4d_trak(self, detector):
        """4D-TRAK keyword."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="mra 4d-trak contrast",
        )
        result = detector.detect(ctx)
        assert result.has("ViewSharing")
    
    def test_fldyn_sequence(self, detector):
        """fldyn sequence pattern."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            stack_sequence_name="*fldyn3d1_ns",
            text_search_blob="dynamic mra",
        )
        result = detector.detect(ctx)
        assert result.has("ViewSharing")
    
    def test_time_resolved(self, detector):
        """Time-resolved keyword."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="time-resolved mra carotid",
        )
        result = detector.detect(ctx)
        assert result.has("ViewSharing")


# =============================================================================
# COMBINED ACCELERATIONS TESTS
# =============================================================================

class TestCombinedAccelerations:
    """Test multiple accelerations detected together."""
    
    def test_pi_plus_pf(self, detector):
        """Parallel imaging + Partial Fourier."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scan_options="['ACC_GEMS', 'PFP']",
            text_search_blob="t1 brain",
        )
        result = detector.detect(ctx)
        assert result.has("ParallelImaging")
        assert result.has("PartialFourier")
        assert len(result.accelerations) >= 2
    
    def test_pi_plus_sms(self, detector):
        """Parallel imaging + SMS."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scan_options="['ACC_GEMS']",
            text_search_blob="dwi grappa mb4 brain",
        )
        result = detector.detect(ctx)
        assert result.has("ParallelImaging")
        assert result.has("SimultaneousMultiSlice")
    
    def test_three_methods(self, detector):
        """Three acceleration methods."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scan_options="['ACC_GEMS', 'PFP']",
            text_search_blob="dwi grappa mb4 partial fourier",
        )
        result = detector.detect(ctx)
        assert result.has("ParallelImaging")
        assert result.has("SimultaneousMultiSlice")
        assert result.has("PartialFourier")
        assert len(result.accelerations) >= 3
    
    def test_pi_plus_vs(self, detector):
        """Parallel imaging + View Sharing."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="mra twist grappa brain",
        )
        result = detector.detect(ctx)
        assert result.has("ParallelImaging")
        assert result.has("ViewSharing")


# =============================================================================
# EXPLAIN DETECTION TESTS
# =============================================================================

class TestExplainDetection:
    """Test explain_detection method."""
    
    def test_explain_no_acceleration(self, detector):
        """Explain with no acceleration."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="brain imaging",
        )
        explanation = detector.explain_detection(ctx)
        assert "No acceleration" in explanation
    
    def test_explain_single_acceleration(self, detector):
        """Explain with single acceleration."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scan_options="['ACC_GEMS']",
            text_search_blob="brain",
        )
        explanation = detector.explain_detection(ctx)
        assert "ParallelImaging" in explanation
        assert "Confidence" in explanation
    
    def test_explain_multiple_accelerations(self, detector):
        """Explain with multiple accelerations."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scan_options="['ACC_GEMS', 'PFP']",
            text_search_blob="brain",
        )
        explanation = detector.explain_detection(ctx)
        assert "ParallelImaging" in explanation
        assert "PartialFourier" in explanation


# =============================================================================
# REAL DATABASE EXAMPLES
# =============================================================================

class TestRealDatabaseExamples:
    """Test with real database fingerprint patterns."""
    
    def test_ge_parallel_imaging(self, detector):
        """GE sequence with ACC_GEMS."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scan_options="['FAST_GEMS', 'EDR_GEMS', 'FILTERED_GEMS', 'ACC_GEMS', 'IR_GEMS']",
            text_search_blob="mpr cor t1 bravo brain",
        )
        result = detector.detect(ctx)
        assert result.has("ParallelImaging")
    
    def test_ge_hypersense_flair(self, detector):
        """GE FLAIR with HyperSense."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scan_options="['SAT_GEMS', 'EDR_GEMS', 'HYPERSENSE_GEMS', 'FILTERED_GEMS', 'ACC_GEMS', 'FS', 'FSS_GEMS', 'IR_GEMS']",
            text_search_blob="sag t2 flair 3d cube brain",
        )
        result = detector.detect(ctx)
        assert result.has("ParallelImaging")
    
    def test_siemens_partial_fourier(self, detector):
        """Siemens with PFP."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scan_options="['IR', 'PFP', 'FS']",
            text_search_blob="t2 flair brain",
        )
        result = detector.detect(ctx)
        assert result.has("PartialFourier")
    
    def test_siemens_pf_both(self, detector):
        """Siemens with both PFP and PFF."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scan_options="['PFP', 'PFF', 'PER']",
            text_search_blob="dwi brain",
        )
        result = detector.detect(ctx)
        assert result.has("PartialFourier")
        accel = result.get("PartialFourier")
        assert "phase" in accel.subtype
        assert "frequency" in accel.subtype


# =============================================================================
# UTILITY METHODS
# =============================================================================

class TestUtilityMethods:
    """Test utility methods."""
    
    def test_get_all_accelerations(self):
        """Get list of all acceleration types."""
        accels = AccelerationDetector.get_all_accelerations()
        assert "ParallelImaging" in accels
        assert "SimultaneousMultiSlice" in accels
        assert "PartialFourier" in accels
        assert "CompressedSensing" in accels
        assert "ViewSharing" in accels
        assert len(accels) == 5
    
    def test_output_has_method(self, detector):
        """Test has() method on output."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scan_options="['ACC_GEMS']",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("ParallelImaging") is True
        assert result.has("CompressedSensing") is False
    
    def test_output_get_method(self, detector):
        """Test get() method on output."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scan_options="['ACC_GEMS']",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        accel = result.get("ParallelImaging")
        assert accel is not None
        assert accel.name == "ParallelImaging"
        
        missing = result.get("CompressedSensing")
        assert missing is None
