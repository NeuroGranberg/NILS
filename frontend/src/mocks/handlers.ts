import { http, HttpResponse, delay } from 'msw';
import type { AnonymizeStageConfig, Cohort, StageId } from '../types';
import { db, runStage, createCohort, updateJobState, findCohort } from './fixtures/database';
import { listDirectories } from './fixtures/filesystem';

const useRealFilesystem = import.meta.env.VITE_USE_REAL_FILES === 'true';

export const handlers = [
  http.get('/api/cohorts', async () => {
    await delay(300);
    return HttpResponse.json(db.cohorts);
  }),
  http.get('/api/cohorts/:cohortId', async ({ params }) => {
    const cohortId = Number(params.cohortId);
    const cohort = findCohort(Number.isNaN(cohortId) ? null : cohortId);
    if (!cohort) {
      return new HttpResponse('Cohort not found', { status: 404 });
    }
    await delay(200);
    return HttpResponse.json(cohort);
  }),
  http.post('/api/cohorts', async ({ request }) => {
    const payload = (await request.json()) as Partial<Cohort> & { anonymizeConfig?: AnonymizeStageConfig };
    const cohort = createCohort(payload);
    await delay(400);
    return HttpResponse.json(cohort, { status: 201 });
  }),
  http.post('/api/cohorts/:cohortId/stages/:stageId/run', async ({ params, request }) => {
    const cohortId = Number(params.cohortId);
    const stageId = String(params.stageId) as StageId;
    const config = ((await request.json()) as Record<string, unknown> | undefined) ?? {};
    if (Number.isNaN(cohortId)) {
      return new HttpResponse('Invalid cohort ID', { status: 400 });
    }
    try {
      const stage = runStage(cohortId, stageId, config);
      await delay(250);
      return HttpResponse.json(stage);
    } catch (error) {
      return new HttpResponse((error as Error).message, { status: 400 });
    }
  }),
  http.get('/api/files', async ({ request }) => {
    const url = new URL(request.url);
    const path = url.searchParams.get('path') ?? '/data';
    
    if (useRealFilesystem) {
      // Fetch from the backend API directly
      const backendUrl = `http://localhost:8000/api/files?path=${encodeURIComponent(path)}`;
      try {
        const response = await fetch(backendUrl);
        if (!response.ok) {
          return new HttpResponse(await response.text(), { status: response.status });
        }
        const data = await response.json();
        return HttpResponse.json(data);
      } catch {
        return new HttpResponse('Failed to fetch directories', { status: 500 });
      }
    }
    
    await delay(200);
    return HttpResponse.json(listDirectories(path));
  }),
  http.get('/api/jobs', async () => {
    await delay(200);
    return HttpResponse.json(db.jobs);
  }),
  http.post('/api/jobs/:jobId/:action', async ({ params }) => {
    const jobId = Number(params.jobId);
    const action = String(params.action) as 'pause' | 'resume' | 'cancel' | 'retry';
    if (Number.isNaN(jobId)) {
      return new HttpResponse('Invalid job ID', { status: 400 });
    }
    try {
      const job = updateJobState(jobId, action);
      await delay(200);
      return HttpResponse.json(job);
    } catch (error) {
      return new HttpResponse((error as Error).message, { status: 400 });
    }
  }),
  http.get('/api/cohorts/:cohortId/examples/folders', async () => {
    // Provide deterministic mock paths for UX previews when MSW is active
    await delay(100);
    return HttpResponse.json({
      paths: [
        'subject01/visitA/series1.dcm',
        'subject02/visitB/series2.dcm',
        'subject02/visitB/series2/001.dcm',
      ],
    });
  }),
];
