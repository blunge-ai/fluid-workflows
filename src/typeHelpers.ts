import { Workflow } from './Workflow';

export type RequireKeys<T, K extends PropertyKey>
  = K extends keyof T ? T : never;

export type WfArray<Names extends string>
  = Workflow<any, any, Names, any, any>[];

export type NamesOfWfs<Wfs extends WfArray<string>>
   = Wfs[number] extends Workflow<any, any, infer N, any, any> ? N : never;

export type QueuesOption<
  Wfs extends WfArray<Names>,
  Names extends string,
  Qs extends Record<NamesOfWfs<Wfs>, string>
> = { readonly queues: RequireKeys<Qs, NamesOfWfs<Wfs>> };

export type ValueOf<T> = T[keyof T];
