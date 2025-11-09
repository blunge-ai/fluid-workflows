import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { Workflow } from '../Workflow';

export type CloudflareWorkflowModuleSpec = {
  /** Identifier exported from the module that resolves to the workflow instance */
  exportName: string;
  /** Module specifier to import from, relative to the generated entry file */
  modulePath: string;
};

export type CloudflareWorkflowDeployTarget = CloudflareWorkflowModuleSpec & {
  /** The workflow instance to deploy */
  workflow: Workflow<any, any, string, any, any>;
  /** Optional override for the remote workflow name */
  remoteName?: string;
  /** Optional override for the generated classname */
  className?: string;
  /** Optional binding name to surface inside the Worker */
  binding?: string;
};

export type CloudflareDeployOptions = {
  /** Directory to write generated Worker source + config into */
  outputDir: string;
  /** Script name registered with Cloudflare */
  scriptName: string;
  /** List of workflows to expose */
  workflows: CloudflareWorkflowDeployTarget[];
  /** Relative path for the generated Worker entry (default: src/index.ts) */
  entryPath?: string;
  /** Wrangler compatibility date (default: 2024-12-27) */
  compatibilityDate?: string;
  /** Optional module specifier for importing Cloudflare helpers (default: fluid-workflows) */
  frameworkImportPath?: string;
  /** Optional additional text appended to wrangler.toml */
  wranglerExtras?: string;
};

const DEFAULT_COMPATIBILITY_DATE = '2024-12-27';
const DEFAULT_ENTRY_PATH = 'src/index.ts';

function ensureUnique<T>(values: T[], label: string) {
  const seen = new Set<T>();
  for (const value of values) {
    if (seen.has(value)) {
      throw new Error(`duplicate ${label}: ${String(value)}`);
    }
    seen.add(value);
  }
}

function toKebab(input: string) {
  return input
    .replace(/[^a-zA-Z0-9]+/g, '-')
    .replace(/-{2,}/g, '-')
    .replace(/^-|-$/g, '')
    .toLowerCase();
}

function toPascal(input: string) {
  const base = input.replace(/[^a-zA-Z0-9]+/g, ' ');
  return base
    .split(' ')
    .filter(Boolean)
    .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1))
    .join('');
}

function defaultRemoteName(workflow: Workflow<any, any, string, any, any>) {
  const base = `${workflow.name}-v${workflow.version}`;
  return toKebab(base);
}

function defaultClassName(remoteName: string) {
  const candidate = toPascal(remoteName);
  return candidate.endsWith('Workflow') ? candidate : `${candidate}Workflow`;
}

function defaultBinding(remoteName: string) {
  return remoteName
    .replace(/[^a-zA-Z0-9]+/g, '_')
    .replace(/_{2,}/g, '_')
    .replace(/^_|_$/g, '')
    .toUpperCase();
}

async function ensureDir(filePath: string) {
  await mkdir(dirname(filePath), { recursive: true });
}

/**
 * Generate a Cloudflare Workers project that exposes the provided workflows as Workflows instances.
 *
 * @example
 * await generateCloudflareDeployment({
 *   outputDir: './dist/cloudflare',
 *   scriptName: 'my-workflows',
 *   workflows: [{
 *     workflow: addWorkflow,
 *     modulePath: '../workflows/add',
 *     exportName: 'addWorkflow',
 *   }],
 * });
 */
export async function generateCloudflareDeployment(options: CloudflareDeployOptions): Promise<void> {
  const {
    outputDir,
    scriptName,
    workflows,
    entryPath = DEFAULT_ENTRY_PATH,
    compatibilityDate = DEFAULT_COMPATIBILITY_DATE,
    frameworkImportPath = 'fluid-workflows',
    wranglerExtras,
  } = options;

  if (!workflows.length) {
    throw new Error('generateCloudflareDeployment requires at least one workflow');
  }

  const resolvedTargets = workflows.map((target) => {
    const remoteName = target.remoteName ?? defaultRemoteName(target.workflow);
    const className = target.className ?? defaultClassName(remoteName);
    const binding = target.binding ?? defaultBinding(remoteName);
    if (!target.modulePath) {
      throw new Error(`modulePath missing for workflow ${target.workflow.name}`);
    }
    if (!target.exportName) {
      throw new Error(`exportName missing for workflow ${target.workflow.name}`);
    }
    return {
      ...target,
      remoteName,
      className,
      binding,
    };
  });

  ensureUnique(resolvedTargets.map((t) => t.remoteName), 'remote workflow name');
  ensureUnique(resolvedTargets.map((t) => t.className), 'class name');
  ensureUnique(resolvedTargets.map((t) => t.binding), 'binding name');

  const importsByModule = new Map<string, Set<string>>();
  for (const { modulePath, exportName } of resolvedTargets) {
    if (!importsByModule.has(modulePath)) {
      importsByModule.set(modulePath, new Set());
    }
    importsByModule.get(modulePath)!.add(exportName);
  }

  const entryFile = join(outputDir, entryPath);
  const wranglerFile = join(outputDir, 'wrangler.toml');

  const importLines = Array.from(importsByModule.entries()).map(([modulePath, names]) => {
    const specifiers = Array.from(names).sort().join(', ');
    return `import { ${specifiers} } from '${modulePath}';`;
  });

  const uniqueWorkflowRefs = Array.from(new Set(resolvedTargets.map((t) => t.exportName)));
  const workflowArrayLiteral = uniqueWorkflowRefs.length > 0
    ? `[${uniqueWorkflowRefs.join(', ')}]`
    : '[]';

  const workflowNameMapEntries = Array.from(new Map(
    resolvedTargets.map((t) => [t.workflow.name, t.remoteName] as const),
  ).entries()).map(([key, value]) => `  '${key}': '${value}',`);
  const workflowNameMapLiteral = workflowNameMapEntries.length > 0
    ? `{
${workflowNameMapEntries.join('\n')}
}`
    : '{}';

  const classBlocks = resolvedTargets.map(({ exportName, className }) => {
    return `export class ${className} extends WorkflowEntrypoint<GenericEnv, unknown> {
  async run(event: CloudflareWorkflowEvent<unknown>, step: CloudflareWorkflowStep) {
    return getRunner(this.env).run(${exportName} as any, event as CloudflareWorkflowEvent<any>, step);
  }
}`;
  });

  const entrySource = `import { WorkflowEntrypoint } from 'cloudflare:workers';
import { CloudflareWorkflowRunner, CloudflareWorkflowDispatcher, type CloudflareWorkflowEvent, type CloudflareWorkflowStep } from '${frameworkImportPath}';
${importLines.join('\n')}

type GenericEnv = Record<string, unknown> & {
  CLOUDFLARE_ACCOUNT_ID: string;
  CLOUDFLARE_API_TOKEN: string;
};

const workflowsList = ${workflowArrayLiteral} as any[];
const workflowNameMap = ${workflowNameMapLiteral};

let cachedRunner: CloudflareWorkflowRunner | undefined;

function getRunner(env: GenericEnv) {
  if (!cachedRunner) {
    const dispatcher = new CloudflareWorkflowDispatcher({
      accountId: env.CLOUDFLARE_ACCOUNT_ID,
      apiToken: env.CLOUDFLARE_API_TOKEN,
      workflows: workflowsList as any,
      workflowNameMap,
    });
    cachedRunner = new CloudflareWorkflowRunner({
      workflows: workflowsList as any,
      dispatcher,
    });
  }
  return cachedRunner;
}

${classBlocks.join('\n\n')}

export default {
  async fetch() {
    return new Response('Not found', { status: 404 });
  },
};
`;

  const wranglerLines: string[] = [
    `name = "${scriptName}"`,
    `main = "${entryPath}"`,
    `compatibility_date = "${compatibilityDate}"`,
    '',
  ];

  for (const { remoteName, className, binding } of resolvedTargets) {
    wranglerLines.push('[[workflows]]');
    wranglerLines.push(`name = "${remoteName}"`);
    wranglerLines.push(`binding = "${binding}"`);
    wranglerLines.push(`class_name = "${className}"`);
    wranglerLines.push('');
  }

  if (wranglerExtras) {
    wranglerLines.push(wranglerExtras.trim(), '');
  }

  await ensureDir(entryFile);
  await writeFile(entryFile, entrySource);
  await writeFile(wranglerFile, wranglerLines.join('\n'));
}
