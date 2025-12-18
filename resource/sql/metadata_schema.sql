-- Canonical schema derived from code_examples/3_database/db_design.md
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS subject (
  subject_id                  INTEGER PRIMARY KEY,
  subject_code                TEXT UNIQUE NOT NULL,
  patient_name                TEXT,
  patient_birth_date          DATE,
  patient_sex                 TEXT,
  ethnic_group                TEXT,
  occupation                  TEXT,
  additional_patient_history  TEXT,
  is_active                   INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
  created_at                  TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at                  TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_subject_is_active ON subject(is_active);

CREATE TABLE IF NOT EXISTS cohort (
  cohort_id     INTEGER PRIMARY KEY,
  name          TEXT UNIQUE NOT NULL,
  owner         TEXT NOT NULL,
  path          TEXT NOT NULL,
  description   TEXT,
  is_active     INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
  created_at    TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cohort_name_unique ON cohort(name);
CREATE INDEX IF NOT EXISTS idx_cohort_is_active ON cohort(is_active);

CREATE TABLE IF NOT EXISTS subject_cohorts (
  subject_cohort_id INTEGER PRIMARY KEY,
  subject_id        INTEGER NOT NULL REFERENCES subject(subject_id) ON DELETE CASCADE,
  cohort_id         INTEGER NOT NULL REFERENCES cohort(cohort_id)   ON DELETE CASCADE,
  notes             TEXT,
  created_at        TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at        TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE(subject_id, cohort_id)
);
CREATE INDEX IF NOT EXISTS idx_subject_cohorts_subject ON subject_cohorts(subject_id);
CREATE INDEX IF NOT EXISTS idx_subject_cohorts_cohort ON subject_cohorts(cohort_id);

CREATE TABLE IF NOT EXISTS id_types (
  id_type_id   INTEGER PRIMARY KEY,
  id_type_name TEXT NOT NULL,
  description  TEXT,
  created_at   TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at   TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE(id_type_name)
);

CREATE TABLE IF NOT EXISTS subject_other_identifiers (
  subject_other_identifier_id INTEGER PRIMARY KEY,
  subject_id                  INTEGER NOT NULL REFERENCES subject(subject_id) ON DELETE CASCADE,
  id_type_id                  INTEGER NOT NULL REFERENCES id_types(id_type_id),
  other_identifier            TEXT NOT NULL,
  created_at                  TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at                  TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE(subject_id, id_type_id)
);
CREATE INDEX IF NOT EXISTS idx_other_ids_lookup ON subject_other_identifiers(id_type_id, other_identifier);

CREATE TABLE IF NOT EXISTS event_types (
  event_type_id  INTEGER PRIMARY KEY,
  event_category TEXT NOT NULL,
  event_name     TEXT NOT NULL,
  description    TEXT,
  created_at     TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at     TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS event (
  event_id     INTEGER PRIMARY KEY,
  subject_id   INTEGER NOT NULL REFERENCES subject(subject_id) ON DELETE CASCADE,
  event_type_id INTEGER NOT NULL REFERENCES event_types(event_type_id),
  event_date   TEXT NOT NULL,
  event_time   TEXT,
  notes        TEXT,
  created_at   TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at   TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_event_subj_date ON event(subject_id, event_date);
CREATE INDEX IF NOT EXISTS idx_event_type_date ON event(event_type_id, event_date);

CREATE TABLE IF NOT EXISTS diseases (
  disease_id   INTEGER PRIMARY KEY,
  disease_name TEXT NOT NULL,
  disease_code TEXT,
  description  TEXT,
  created_at   TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS disease_types (
  disease_type_id INTEGER PRIMARY KEY,
  disease_id      INTEGER NOT NULL REFERENCES diseases(disease_id),
  type_name       TEXT NOT NULL,
  description     TEXT,
  sort_order      INTEGER,
  created_at      TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS subject_diseases (
  subject_disease_id INTEGER PRIMARY KEY,
  subject_id         INTEGER NOT NULL REFERENCES subject(subject_id) ON DELETE CASCADE,
  disease_id         INTEGER NOT NULL REFERENCES diseases(disease_id),
  diagnosis_notes    TEXT,
  family_history     TEXT,
  onset_event_id     INTEGER REFERENCES event(event_id),
  diagnosis_event_id INTEGER REFERENCES event(event_id),
  is_active          INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
  created_at         TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at         TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE(subject_id, disease_id)
);

CREATE TABLE IF NOT EXISTS subject_disease_types (
  subject_disease_type_id INTEGER PRIMARY KEY,
  subject_disease_id      INTEGER NOT NULL REFERENCES subject_diseases(subject_disease_id) ON DELETE CASCADE,
  disease_type_id         INTEGER NOT NULL REFERENCES disease_types(disease_type_id),
  assignment_date         TEXT NOT NULL,
  transition_event_id     INTEGER REFERENCES event(event_id),
  notes                   TEXT,
  is_active               INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
  created_at              TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at              TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_sdt_assign ON subject_disease_types(subject_disease_id, assignment_date);
CREATE INDEX IF NOT EXISTS idx_sdt_type_active ON subject_disease_types(disease_type_id, is_active);

CREATE TABLE IF NOT EXISTS clinical_measure_types (
  measure_type_id INTEGER PRIMARY KEY,
  category_name   TEXT NOT NULL,
  measure_name    TEXT NOT NULL,
  description     TEXT,
  unit            TEXT,
  value_type      TEXT,
  min_value       REAL,
  max_value       REAL,
  is_active       INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
  is_primary      INTEGER NOT NULL DEFAULT 0 CHECK(is_primary IN (0,1)),
  created_at      TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS numeric_measures (
  measure_id      INTEGER PRIMARY KEY,
  subject_id      INTEGER NOT NULL REFERENCES subject(subject_id) ON DELETE CASCADE,
  measure_type_id INTEGER NOT NULL REFERENCES clinical_measure_types(measure_type_id),
  numeric_value   REAL NOT NULL,
  unit            TEXT,
  source_system   TEXT,
  quality_flag    TEXT,
  notes           TEXT,
  event_id        INTEGER REFERENCES event(event_id),
  created_at      TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_num_meas ON numeric_measures(subject_id, measure_type_id);
CREATE INDEX IF NOT EXISTS idx_num_meas_event ON numeric_measures(event_id);
CREATE INDEX IF NOT EXISTS idx_num_meas_value ON numeric_measures(numeric_value);

CREATE TABLE IF NOT EXISTS text_measures (
  measure_id      INTEGER PRIMARY KEY,
  subject_id      INTEGER NOT NULL REFERENCES subject(subject_id) ON DELETE CASCADE,
  measure_type_id INTEGER NOT NULL REFERENCES clinical_measure_types(measure_type_id),
  text_value      TEXT NOT NULL,
  source_system   TEXT,
  quality_flag    TEXT,
  notes           TEXT,
  event_id        INTEGER REFERENCES event(event_id),
  created_at      TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_text_meas ON text_measures(subject_id, measure_type_id);
CREATE INDEX IF NOT EXISTS idx_text_meas_event ON text_measures(event_id);

CREATE TABLE IF NOT EXISTS boolean_measures (
  measure_id      INTEGER PRIMARY KEY,
  subject_id      INTEGER NOT NULL REFERENCES subject(subject_id) ON DELETE CASCADE,
  measure_type_id INTEGER NOT NULL REFERENCES clinical_measure_types(measure_type_id),
  boolean_value   INTEGER NOT NULL CHECK(boolean_value IN (0,1)),
  source_system   TEXT,
  quality_flag    TEXT,
  notes           TEXT,
  event_id        INTEGER REFERENCES event(event_id),
  created_at      TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_bool_meas ON boolean_measures(subject_id, measure_type_id);
CREATE INDEX IF NOT EXISTS idx_bool_meas_event ON boolean_measures(event_id);
CREATE INDEX IF NOT EXISTS idx_bool_meas_value ON boolean_measures(boolean_value);

CREATE TABLE IF NOT EXISTS json_measures (
  measure_id      INTEGER PRIMARY KEY,
  subject_id      INTEGER NOT NULL REFERENCES subject(subject_id) ON DELETE CASCADE,
  measure_type_id INTEGER NOT NULL REFERENCES clinical_measure_types(measure_type_id),
  json_value      TEXT NOT NULL,
  source_system   TEXT,
  quality_flag    TEXT,
  notes           TEXT,
  event_id        INTEGER REFERENCES event(event_id),
  created_at      TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_json_meas ON json_measures(subject_id, measure_type_id);
CREATE INDEX IF NOT EXISTS idx_json_meas_event ON json_measures(event_id);

CREATE TABLE IF NOT EXISTS study (
  study_id                INTEGER PRIMARY KEY,
  study_instance_uid      TEXT UNIQUE NOT NULL,
  study_date              TEXT,
  study_time              TEXT,
  study_description       TEXT,
  study_comments          TEXT,
  modality                TEXT,
  manufacturer            TEXT,
  manufacturer_model_name TEXT,
  station_name            TEXT,
  institution_name        TEXT,
  subject_id              INTEGER NOT NULL REFERENCES subject(subject_id) ON DELETE CASCADE,
  quality_control         TEXT
);
CREATE INDEX IF NOT EXISTS idx_study_subject ON study(subject_id);
CREATE INDEX IF NOT EXISTS idx_study_modality_date ON study(modality, study_date);

CREATE TABLE IF NOT EXISTS series (
  series_id                       INTEGER PRIMARY KEY,
  series_instance_uid             TEXT UNIQUE NOT NULL,
  frame_of_reference_uid          TEXT,
  implementation_class_uid        TEXT,
  media_storage_sop_instance_uid  TEXT,
  sop_class_uid                   TEXT,
  implementation_version_name     TEXT,
  series_date                     TEXT,
  series_time                     TEXT,
  modality                        TEXT NOT NULL,
  image_type                      TEXT,
  sequence_name                   TEXT,
  protocol_name                   TEXT,
  series_description              TEXT,
  body_part_examined              TEXT,
  scanning_sequence               TEXT,
  sequence_variant                TEXT,
  scan_options                    TEXT,
  series_comments                 TEXT,
  slice_thickness                 REAL,
  spacing_between_slices          REAL,
  images_in_acquisition           TEXT,
  image_orientation_patient       TEXT,
  image_position_patient          TEXT,
  patient_position                TEXT,
  contrast_bolus_agent            TEXT,
  contrast_bolus_route            TEXT,
  contrast_bolus_total_dose       REAL,
  contrast_bolus_start_time       TEXT,
  contrast_bolus_volume           REAL,
  contrast_flow_rate              REAL,
  contrast_flow_duration          REAL,
  study_id                        INTEGER NOT NULL REFERENCES study(study_id) ON DELETE CASCADE,
  subject_id                      INTEGER NOT NULL REFERENCES subject(subject_id) ON DELETE CASCADE,
  quality_control                 TEXT,
  processing_status               TEXT,
  acquisition_compliance          TEXT
);
CREATE INDEX IF NOT EXISTS idx_series_study    ON series(study_id);
CREATE INDEX IF NOT EXISTS idx_series_subject  ON series(subject_id);
CREATE INDEX IF NOT EXISTS idx_series_modality ON series(modality);
CREATE INDEX IF NOT EXISTS idx_series_bodypart ON series(body_part_examined);

CREATE TABLE IF NOT EXISTS mri_series_details (
  series_id                        INTEGER PRIMARY KEY REFERENCES series(series_id) ON DELETE CASCADE,
  series_instance_uid              TEXT UNIQUE NOT NULL,
  mr_acquisition_type              TEXT,
  angio_flag                       TEXT,
  repetition_time                  REAL,
  echo_time                        REAL,
  inversion_time                   REAL,
  inversion_times                  TEXT,
  flip_angle                       REAL,
  phase_contrast                   TEXT,
  number_of_averages               REAL,
  imaging_frequency                REAL,
  imaged_nucleus                   TEXT,
  echo_numbers                     TEXT,
  magnetic_field_strength          REAL,
  number_of_phase_encoding_steps   TEXT,
  echo_train_length                INTEGER,
  percent_sampling                 REAL,
  percent_phase_field_of_view      REAL,
  pixel_bandwidth                  TEXT,
  receive_coil_name                TEXT,
  transmit_coil_name               TEXT,
  acquisition_matrix               TEXT,
  phase_encoding_direction         TEXT,
  sar                              REAL,
  dbdt                             TEXT,
  b1rms                            TEXT,
  temporal_position_identifier     TEXT,
  number_of_temporal_positions     TEXT,
  temporal_resolution              TEXT,
  diffusion_b_value                TEXT,
  diffusion_gradient_orientation   TEXT,
  diffusion_directionality         TEXT,
  parallel_acquisition_technique   TEXT,
  parallel_reduction_factor_in_plane TEXT
);

CREATE TABLE IF NOT EXISTS ct_series_details (
  series_id                        INTEGER PRIMARY KEY REFERENCES series(series_id) ON DELETE CASCADE,
  series_instance_uid              TEXT UNIQUE NOT NULL,
  kvp                               REAL,
  data_collection_diameter          REAL,
  reconstruction_diameter           REAL,
  gantry_detector_tilt              REAL,
  table_height                      REAL,
  rotation_direction                TEXT,
  exposure_time                     REAL,
  x_ray_tube_current                REAL,
  exposure                          REAL,
  filter_type                       TEXT,
  generator_power                   REAL,
  focal_spots                       TEXT,
  convolution_kernel                TEXT,
  revolution_time                   REAL,
  single_collimation_width          REAL,
  total_collimation_width           REAL,
  table_speed                       REAL,
  table_feed_per_rotation           REAL,
  spiral_pitch_factor               REAL,
  exposure_modulation_type          TEXT,
  ctdi_vol                          REAL,
  ctdi_phantom_type_code_sequence   TEXT,
  calcium_scoring_mass_factor_device TEXT,
  calcium_scoring_mass_factor_patient REAL
);

CREATE TABLE IF NOT EXISTS pet_series_details (
  series_id                         INTEGER PRIMARY KEY REFERENCES series(series_id) ON DELETE CASCADE,
  series_instance_uid               TEXT UNIQUE NOT NULL,
  radiopharmaceutical               TEXT,
  radionuclide_total_dose           REAL,
  radionuclide_half_life            REAL,
  radionuclide_positron_fraction    REAL,
  radiopharmaceutical_start_time    TEXT,
  radiopharmaceutical_stop_time     TEXT,
  radiopharmaceutical_volume        REAL,
  radiopharmaceutical_route         TEXT,
  decay_correction                  TEXT,
  decay_factor                      REAL,
  reconstruction_method             TEXT,
  scatter_correction_method         TEXT,
  attenuation_correction_method     TEXT,
  randoms_correction_method         TEXT,
  dose_calibration_factor           REAL,
  activity_concentration_scale      REAL,
  suv_type                          TEXT,
  suvbw                             REAL,
  suvlbm                            REAL,
  suvbsa                            REAL,
  counts_source                     TEXT,
  units                             TEXT,
  frame_reference_time              REAL,
  actual_frame_duration             REAL,
  patient_gantry_relationship_code  TEXT,
  slice_progression_direction       TEXT,
  series_type                       TEXT,
  units_type                        TEXT,
  counts_included                   TEXT
);

CREATE TABLE IF NOT EXISTS instance (
  instance_id           INTEGER PRIMARY KEY,
  series_id             INTEGER NOT NULL REFERENCES series(series_id) ON DELETE CASCADE,
  series_instance_uid   TEXT NOT NULL,
  sop_instance_uid      TEXT UNIQUE NOT NULL,
  instance_number       INTEGER,
  acquisition_number    INTEGER,
  acquisition_date      TEXT,
  acquisition_time      TEXT,
  content_date          TEXT,
  content_time          TEXT,
  slice_location        REAL,
  pixel_spacing         TEXT,
  rows                  INTEGER,
  columns               INTEGER,
  bits_allocated        INTEGER,
  bits_stored           INTEGER,
  high_bit              INTEGER,
  pixel_representation  INTEGER,
  window_center         TEXT,
  window_width          TEXT,
  rescale_intercept     REAL,
  rescale_slope         REAL,
  number_of_frames      INTEGER,
  lossy_image_compression TEXT,
  derivation_description TEXT,
  image_comments        TEXT,
  transfer_syntax_uid   TEXT,
  dicom_file_path       TEXT,
  quality_control       TEXT
);
CREATE INDEX IF NOT EXISTS idx_instance_series ON instance(series_id);
CREATE INDEX IF NOT EXISTS idx_instance_sop    ON instance(sop_instance_uid);

CREATE TABLE IF NOT EXISTS instance_frame (
  frame_id            INTEGER PRIMARY KEY,
  instance_id         INTEGER NOT NULL REFERENCES instance(instance_id) ON DELETE CASCADE,
  frame_number        INTEGER NOT NULL,
  image_position      TEXT,
  image_orientation   TEXT,
  slice_location      REAL,
  b_value             REAL,
  echo_time           REAL,
  inversion_time      REAL,
  flip_angle          REAL,
  contrast_frame_flag TEXT
);
CREATE INDEX IF NOT EXISTS idx_frame_instance ON instance_frame(instance_id);

CREATE TABLE IF NOT EXISTS series_classification_cache (
  series_id               INTEGER PRIMARY KEY REFERENCES series(series_id) ON DELETE CASCADE,
  series_instance_uid     TEXT UNIQUE NOT NULL,
  dicom_origin_cohort     TEXT,
  classification_string   TEXT,
  unique_series_under_string INTEGER,
  fov_x_mm                REAL,
  fov_y_mm                REAL,
  aspect_ratio            REAL,
  slices_count            INTEGER,
  rows                    INTEGER,
  columns                 INTEGER,
  pixsp_row_mm            REAL,
  pixsp_col_mm            REAL,
  orientation_patient     TEXT,
  echo_number             INTEGER,
  directory_type          TEXT,
  base                    TEXT,
  modifier_csv            TEXT,
  technique               TEXT,
  construct_csv           TEXT,
  provenance_csv          TEXT,
  acceleration_csv        TEXT,
  post_contrast           INTEGER CHECK(post_contrast IN (0,1)),
  localizer               INTEGER CHECK(localizer IN (0,1)),
  spinal_cord             INTEGER CHECK(spinal_cord IN (0,1)),
  study_id                INTEGER REFERENCES study(study_id)   ON DELETE CASCADE,
  subject_id              INTEGER REFERENCES subject(subject_id) ON DELETE CASCADE,
  manual_review_required  INTEGER CHECK(manual_review_required IN (0,1)),
  manual_review_reasons_csv TEXT
);
CREATE INDEX IF NOT EXISTS idx_scc_subject    ON series_classification_cache(subject_id);
CREATE INDEX IF NOT EXISTS idx_scc_contrast   ON series_classification_cache(post_contrast);
CREATE INDEX IF NOT EXISTS idx_scc_base_tech  ON series_classification_cache(base, technique);

CREATE TABLE IF NOT EXISTS ingest_conflicts (
  id INTEGER PRIMARY KEY,
  cohort_id INTEGER NOT NULL,
  scope TEXT NOT NULL,
  uid TEXT NOT NULL,
  message TEXT NOT NULL,
  file_path TEXT,
  resolved INTEGER NOT NULL DEFAULT 0 CHECK(resolved IN (0,1))
);
