from extract.progress import ExtractionProgressTracker


def test_progress_tracker_handles_resume_scenario():
    emissions: list[int] = []
    tracker = ExtractionProgressTracker(emissions.append)

    tracker.update(5, 10)  # baseline
    tracker.update(7, 10)
    tracker.update(10, 10)
    tracker.finalize()

    assert emissions == [50, 70, 100]


def test_progress_tracker_finalize_forces_completion():
    emissions: list[int] = []
    tracker = ExtractionProgressTracker(emissions.append)

    tracker.update(0, 10)
    tracker.finalize()

    assert emissions == [0, 100]
