import { ref, onBeforeUnmount, type Ref } from 'vue'
import { getStreamWspUrl, getStreamBaseUrl } from '../utils/streamUrls'
import { WebGLVideoRenderer, type RenderEngine } from '../utils/webglVideoRenderer'
import { parseNaluStream } from '../utils/naluParser'

export interface WSPPlayerState {
  isPlaying: Ref<boolean>
  isConnecting: Ref<boolean>
  error: Ref<string | null>
  decoderType: Ref<'hardware' | 'software' | 'none'>
  renderEngine: Ref<RenderEngine>
  detectedCodec: Ref<string>
  start: (streamId: string, isHero?: boolean, hasSub?: boolean) => Promise<void>
  stop: () => void
}

export function useWSPCanvasPlayer(canvasRef: Ref<HTMLCanvasElement | null>): WSPPlayerState {
  const isPlaying = ref(false)
  const isConnecting = ref(false)
  const error = ref<string | null>(null)
  const decoderType = ref<'hardware' | 'software' | 'none'>('hardware')
  const renderEngine = ref<RenderEngine>('webgl')
  const detectedCodec = ref<string>('H.264')

  let ws: WebSocket | null = null
  let decoder: any = null
  let renderer: WebGLVideoRenderer | null = null
  let isStopped = false
  let activeStreamId = ''
  let activeIsHero = false
  let activeHasSub = true
  let activeTargetId = ''
  let hasKeyframe = false
  let pendingFrame: any = null
  let rafId: number | null = null
  let currentConfiguredCodec = ''
  let isStarting = false

  const ensureRenderer = () => {
    if (!renderer && canvasRef.value) {
      renderer = new WebGLVideoRenderer(canvasRef.value)
      renderEngine.value = renderer.getEngine()
    }
  }

  const renderLoop = () => {
    if (isStopped) return
    if (pendingFrame && canvasRef.value) {
      const f = pendingFrame
      pendingFrame = null
      ensureRenderer()
      if (renderer) {
        renderer.renderVideoFrame(f)
      }
      f.close()
    }
    rafId = requestAnimationFrame(renderLoop)
  }

  const stop = () => {
    isStopped = true
    isStarting = false
    hasKeyframe = false
    activeTargetId = ''
    currentConfiguredCodec = ''

    if (rafId) {
      cancelAnimationFrame(rafId)
      rafId = null
    }
    if (pendingFrame) {
      try { pendingFrame.close() } catch (_) {}
      pendingFrame = null
    }
    if (ws) {
      ws.onmessage = null
      ws.onerror = null
      ws.onclose = null
      try { ws.close() } catch (_) {}
      ws = null
    }
    if (decoder) {
      try { decoder.close() } catch (_) {}
      decoder = null
    }
    if (renderer) {
      renderer.destroy()
      renderer = null
    }

    isPlaying.value = false
    isConnecting.value = false
    decoderType.value = 'none'
  }

  const initDecoder = (codecString: string) => {
    if (typeof window === 'undefined' || !(window as any).VideoDecoder) {
      decoderType.value = 'none'
      return null
    }

    try {
      const dec = new (window as any).VideoDecoder({
        output: (frame: any) => {
          if (!canvasRef.value || isStopped) {
            frame.close()
            return
          }
          if (pendingFrame) {
            pendingFrame.close()
          }
          pendingFrame = frame
          isPlaying.value = true
          isConnecting.value = false
          decoderType.value = 'hardware'
        },
        error: (e: any) => {
          console.warn('[HydraWebCodecs GPU] Decode error:', e?.message || e)
          error.value = e?.message || 'Decode error'
          try { dec.close() } catch (_) {}
          decoder = null
          decoderType.value = 'none'
        }
      })

      dec.configure({
        codec: codecString,
        optimizeForLatency: true,
        hardwareAcceleration: 'prefer-hardware'
      })

      currentConfiguredCodec = codecString
      return dec
    } catch (e: any) {
      console.warn('[HydraWebCodecs] Configuration failed for', codecString, e?.message)
      error.value = e?.message || 'VideoDecoder failed'
      decoderType.value = 'none'
      return null
    }
  }

  const start = async (streamId: string, isHero = false, hasSub = true) => {
    if (!canvasRef.value || !streamId) {
      stop()
      return
    }

    const targetId = (!isHero && hasSub) ? `${streamId}_sub` : streamId
    if (isStarting && activeTargetId === targetId) return
    if (activeTargetId === targetId && activeIsHero === isHero && ws &&
        (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) && !isStopped) {
      return
    }

    stop()
    isStopped = false
    isStarting = true
    hasKeyframe = false
    activeStreamId = streamId
    activeIsHero = isHero
    activeHasSub = hasSub
    activeTargetId = targetId
    isConnecting.value = true
    error.value = null

    ensureRenderer()

    // Default starting codec
    const defaultCodec = isHero ? 'avc1.640033' : 'avc1.42c01f'
    decoder = initDecoder(defaultCodec)
    if (!decoder) {
      error.value = 'WebCodecs not supported on this browser'
      isConnecting.value = false
      isStarting = false
      return
    }

    if (!rafId) {
      rafId = requestAnimationFrame(renderLoop)
    }

    let frameSeq = 0
    try {
      // Trigger camera pipeline start on HydraStream data plane
      await fetch(`${getStreamBaseUrl()}/api/v1/streams/${encodeURIComponent(targetId)}/h264/start`, { method: 'POST' }).catch(() => {})
      if (isStopped || activeTargetId !== targetId) return

      ws = new WebSocket(getStreamWspUrl(streamId, isHero, hasSub))
      ws.binaryType = 'arraybuffer'

      ws.onmessage = (event) => {
        if (isStopped || !(event.data instanceof ArrayBuffer) || event.data.byteLength <= 9) return

        const buf = event.data as ArrayBuffer
        const view = new DataView(buf)
        const isKey = view.getUint8(0) === 0x01

        if (!isKey && !hasKeyframe) return
        hasKeyframe = true

        const nalu = new Uint8Array(buf, 9)

        // Dynamic Codec inspection on keyframe
        if (isKey) {
          const parsed = parseNaluStream(nalu)
          detectedCodec.value = parsed.codecType === 'h265' ? 'H.265' : 'H.264'

          if (parsed.codecString && parsed.codecString !== currentConfiguredCodec) {
            try {
              if (decoder && decoder.state === 'configured') {
                decoder.configure({
                  codec: parsed.codecString,
                  optimizeForLatency: true,
                  hardwareAcceleration: 'prefer-hardware'
                })
                currentConfiguredCodec = parsed.codecString
              }
            } catch (_) {
              // Keep current decoder configuration if reconfigure fails
            }
          }
        }

        if (!decoder || decoder.state !== 'configured') {
          decoder = initDecoder(currentConfiguredCodec || defaultCodec)
          if (!decoder || !isKey) return
        }

        const ts = Math.round(performance.now() * 1000) + (frameSeq++)
        try {
          decoder.decode(new (window as any).EncodedVideoChunk({
            type: isKey ? 'key' : 'delta',
            timestamp: ts,
            data: nalu
          }))
        } catch (decErr: any) {
          console.debug('[HydraDecoder] Dropped unparseable frame chunk', decErr?.message)
        }
      }

      ws.onerror = () => {
        if (!isStopped) {
          error.value = 'WSP connection error'
          isConnecting.value = false
        }
      }

      ws.onclose = () => {
        if (!isStopped && !document.hidden && activeStreamId) {
          setTimeout(() => {
            if (!isStopped) start(activeStreamId, activeIsHero, activeHasSub)
          }, 1000)
        }
      }
    } catch (err: any) {
      error.value = err?.message || 'WebSocket init failed'
      isConnecting.value = false
    } finally {
      isStarting = false
    }
  }

  const handleVis = () => {
    if (!document.hidden && activeStreamId && (!ws || ws.readyState === WebSocket.CLOSED)) {
      start(activeStreamId, activeIsHero, activeHasSub)
    }
  }

  document.addEventListener('visibilitychange', handleVis)
  window.addEventListener('focus', handleVis)

  onBeforeUnmount(() => {
    document.removeEventListener('visibilitychange', handleVis)
    window.removeEventListener('focus', handleVis)
    stop()
  })

  return {
    isPlaying,
    isConnecting,
    error,
    decoderType,
    renderEngine,
    detectedCodec,
    start,
    stop
  }
}
