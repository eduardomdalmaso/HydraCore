/**
 * Lightweight NALU Parser & Codec Detector for HydraVMS.
 *
 * Automatically parses H.264 and H.265/HEVC bitstream headers to generate
 * valid MIME codec strings for the WebCodecs VideoDecoder API.
 */

export interface CodecInfo {
  codecType: 'h264' | 'h265' | 'unknown'
  codecString: string
  isKeyframe: boolean
  profileName?: string
  width?: number
  height?: number
}

export function parseNaluStream(data: Uint8Array): CodecInfo {
  let offset = 0
  const len = data.length

  // Find start code (0x000001 or 0x00000001)
  while (offset + 4 < len) {
    if (data[offset] === 0x00 && data[offset + 1] === 0x00) {
      if (data[offset + 2] === 0x01) {
        offset += 3
        break
      } else if (data[offset + 2] === 0x00 && data[offset + 3] === 0x01) {
        offset += 4
        break
      }
    }
    offset++
  }

  if (offset >= len) {
    return { codecType: 'unknown', codecString: 'avc1.42c01f', isKeyframe: false }
  }

  const headerByte = data[offset]

  // 1. Check for H.265 (HEVC)
  // In H.265, nal_unit_type is 6 bits: (byte >> 1) & 0x3F
  // 32 = VPS, 33 = SPS, 34 = PPS, 19/20 = IDR
  const hevcType = (headerByte >> 1) & 0x3f
  if (hevcType === 32 || hevcType === 33 || hevcType === 34 || hevcType === 19 || hevcType === 20) {
    const isKey = hevcType === 32 || hevcType === 33 || hevcType === 19 || hevcType === 20
    return {
      codecType: 'h265',
      codecString: 'hev1.1.6.L93.B0',
      isKeyframe: isKey,
      profileName: 'HEVC Main Profile'
    }
  }

  // 2. Check for H.264 (AVC)
  // In H.264, nal_unit_type is 5 bits: byte & 0x1F
  // 7 = SPS, 8 = PPS, 5 = IDR, 1 = non-IDR
  const avcType = headerByte & 0x1f
  if (avcType === 7) {
    // SPS contains profile_idc, constraint_set_flags, level_idc at offset+1, offset+2, offset+3
    if (offset + 3 < len) {
      const profile = data[offset + 1].toString(16).padStart(2, '0')
      const compat = data[offset + 2].toString(16).padStart(2, '0')
      const level = data[offset + 3].toString(16).padStart(2, '0')
      const codecString = `avc1.${profile}${compat}${level}`
      return {
        codecType: 'h264',
        codecString,
        isKeyframe: true,
        profileName: getH264ProfileName(data[offset + 1])
      }
    }
  }

  const isKey = avcType === 5 || avcType === 7 || avcType === 8
  return {
    codecType: 'h264',
    codecString: 'avc1.42c01f',
    isKeyframe: isKey,
    profileName: 'H.264 Baseline'
  }
}

function getH264ProfileName(profileIdc: number): string {
  switch (profileIdc) {
    case 66: return 'Baseline'
    case 77: return 'Main'
    case 100: return 'High'
    case 110: return 'High 10'
    default: return `Profile ${profileIdc}`
  }
}
