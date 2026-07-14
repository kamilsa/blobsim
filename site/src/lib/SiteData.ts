import fs from 'node:fs';
import path from 'node:path';

export interface RunMetadata {
  total_nodes?: number;
  clients?: Record<string, number>;
  status?: string;
  duration_secs?: number;
  runner?: string;
  seed?: number;
}

export interface NotebookData {
  html_path: string;
}

export interface RunData {
  rendered_at: string;
  notebooks: Record<string, NotebookData>;
  metadata: RunMetadata;
}

export interface Manifest {
  runs: Record<string, RunData>;
}

export class SiteData {
  private readonly manifest: Manifest;

  private constructor(manifest: Manifest) {
    this.manifest = manifest;
  }

  static load(): SiteData {
    return new SiteData(SiteData.loadManifest());
  }

  private static loadManifest(): Manifest {
    const manifestPath = path.join(process.cwd(), 'rendered', 'manifest.json');
    try {
      if (fs.existsSync(manifestPath)) {
        const content = fs.readFileSync(manifestPath, 'utf-8');
        const parsed = JSON.parse(content);
        return { runs: {}, ...parsed };
      }
    } catch (e) {
      console.error('Failed to load manifest.json', e);
    }
    return { runs: {} };
  }

  /** All runs sorted by rendered_at (newest first) */
  get runs(): Array<RunData & { run_id: string }> {
    return Object.entries(this.manifest.runs)
      .map(([run_id, data]) => ({ run_id, ...data }))
      .sort((a, b) => b.rendered_at.localeCompare(a.rendered_at));
  }

  /** Get a specific run */
  getRun(runId: string): (RunData & { run_id: string }) | undefined {
    const data = this.manifest.runs[runId];
    if (!data) return undefined;
    return { run_id: runId, ...data };
  }

  /** Total run count */
  get totalRuns(): number {
    return Object.keys(this.manifest.runs).length;
  }
}
