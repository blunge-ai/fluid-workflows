import type { Workflow } from './Workflow';
import type { MatchingWorkflow, NamesOfWfs, WfArray } from './typeHelpers';

export type DispatchOptions<Meta> = {
  jobId?: string,
  meta?: Meta,
};

export interface WorkflowDispatcher<
  Names extends NamesOfWfs<Wfs>,
  Wfs extends WfArray<Names>
> {
  dispatch<N extends string, Input, Output, No, Co, Meta = unknown>(
    props: MatchingWorkflow<Workflow<Input, Output, N, No, Co>, Names, Input, Output, No, Co>,
    input: Input,
    opts?: DispatchOptions<Meta>,
  ): Promise<void>;

  dispatchAwaitingOutput<N extends string, Input, Output, No, Co, Meta = unknown>(
    props: MatchingWorkflow<Workflow<Input, Output, N, No, Co>, Names, Input, Output, No, Co>,
    input: Input,
    opts?: DispatchOptions<Meta>,
  ): Promise<Output>;
}
