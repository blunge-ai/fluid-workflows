import { Workflow } from './Workflow';

export type WorkflowsArray<Names extends string> = Workflow<any, any, Names>[];

export type RequireExactKeys<TObj, K extends PropertyKey> = Exclude<keyof TObj, K> extends never
  ? (Exclude<K, keyof TObj> extends never ? TObj : never)
  : never;

// TODO I think we can simplify to type NamesOf<Wfs extends WorkflowsArray<string>> = Wfs[number] extends Workflow<any, any, infer N> ? N : never;
export type NamesOf<A extends WorkflowsArray<Names>, Names extends string> = A[number] extends infer U
  ? U extends Workflow<any, any, infer N> ? N : never
  : never;

export type QueuesOption<A extends WorkflowsArray<Names>, Names extends string, Qs extends Record<NamesOf<A, Names>, string>>
  = { readonly queues: RequireExactKeys<Qs, NamesOf<A, Names>> };

export type ValueOf<T> = T[keyof T];
