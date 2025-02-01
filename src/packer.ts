import { Packr, isNativeAccelerationEnabled } from 'msgpackr';

if (!isNativeAccelerationEnabled) {
  console.warn('Native acceleration not enabled!!')
}

const packer = new Packr({
  useRecords: false,
  encodeUndefinedAsNil: true,
});

// eslint-disable-next-line
export const pack = packer.pack;
// eslint-disable-next-line
export const unpack = packer.unpack;
