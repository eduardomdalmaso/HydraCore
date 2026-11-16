/**
 * High-Performance WebGL/WebGL2 Zero-Copy Video Renderer for HydraVMS.
 *
 * Implements the Vezha VMS hardware pipeline:
 * - Direct GPU texture upload from WebCodecs VideoFrame (zero CPU copies).
 * - Multi-planar YUV420p shader execution on GPU for software/WASM decoders.
 * - Automatic canvas scaling and aspect preservation.
 * - Context-loss resilience with automatic recreation or 2D fallback.
 */

const VERTEX_SHADER_SOURCE = `
attribute vec2 a_position;
attribute vec2 a_texCoord;
varying vec2 v_texCoord;

void main() {
  gl_Position = vec4(a_position, 0.0, 1.0);
  v_texCoord = a_texCoord;
}
`

const FRAGMENT_SHADER_RGBA = `
precision mediump float;
varying vec2 v_texCoord;
uniform sampler2D u_image;

void main() {
  gl_FragColor = texture2D(u_image, v_texCoord);
}
`

const FRAGMENT_SHADER_YUV = `
precision mediump float;
varying vec2 v_texCoord;
uniform sampler2D u_textureY;
uniform sampler2D u_textureU;
uniform sampler2D u_textureV;

void main() {
  float y = texture2D(u_textureY, v_texCoord).r;
  float u = texture2D(u_textureU, v_texCoord).r - 0.5;
  float v = texture2D(u_textureV, v_texCoord).r - 0.5;

  // BT.601 standard color matrix
  float r = y + 1.402 * v;
  float g = y - 0.344136 * u - 0.714136 * v;
  float b = y + 1.772 * u;

  gl_FragColor = vec4(r, g, b, 1.0);
}
`

export type RenderEngine = 'webgl2' | 'webgl' | 'canvas2d'

export class WebGLVideoRenderer {
  private canvas: HTMLCanvasElement
  private gl: WebGLRenderingContext | WebGL2RenderingContext | null = null
  private ctx2d: CanvasRenderingContext2D | null = null
  private engine: RenderEngine = 'canvas2d'

  private programRgba: WebGLProgram | null = null
  private programYuv: WebGLProgram | null = null

  private positionBuffer: WebGLBuffer | null = null
  private texCoordBuffer: WebGLBuffer | null = null

  private textureRgba: WebGLTexture | null = null
  private textureY: WebGLTexture | null = null
  private textureU: WebGLTexture | null = null
  private textureV: WebGLTexture | null = null

  private isContextLost = false
  private boundContextLostHandler: (e: Event) => void
  private boundContextRestoredHandler: (e: Event) => void

  constructor(canvas: HTMLCanvasElement) {
    this.canvas = canvas
    this.boundContextLostHandler = this.onContextLost.bind(this)
    this.boundContextRestoredHandler = this.onContextRestored.bind(this)
    this.init()
  }

  private init() {
    this.cleanupGL()

    // 1. Try WebGL2 first, then WebGL1
    const glOptions: WebGLContextAttributes = {
      alpha: false,
      depth: false,
      stencil: false,
      antialias: false,
      preserveDrawingBuffer: false,
      powerPreference: 'high-performance'
    }

    try {
      this.gl = (this.canvas.getContext('webgl2', glOptions) as WebGL2RenderingContext) ||
                (this.canvas.getContext('webgl', glOptions) as WebGLRenderingContext)
    } catch {
      this.gl = null
    }

    if (this.gl) {
      this.engine = this.gl instanceof (window as any).WebGL2RenderingContext ? 'webgl2' : 'webgl'
      this.canvas.addEventListener('webglcontextlost', this.boundContextLostHandler, false)
      this.canvas.addEventListener('webglcontextrestored', this.boundContextRestoredHandler, false)
      this.setupShaders()
    } else {
      // Fallback: 2D canvas context
      this.engine = 'canvas2d'
      this.ctx2d = this.canvas.getContext('2d', { alpha: false })
    }
  }

  private onContextLost(e: Event) {
    e.preventDefault()
    this.isContextLost = true
    console.warn('[HydraWebGL] WebGL context lost. Suspending GPU rendering.')
  }

  private onContextRestored() {
    console.info('[HydraWebGL] WebGL context restored. Recompiling GPU shaders.')
    this.isContextLost = false
    this.init()
  }

  private createShader(type: number, source: string): WebGLShader | null {
    if (!this.gl) return null
    const shader = this.gl.createShader(type)
    if (!shader) return null
    this.gl.shaderSource(shader, source)
    this.gl.compileShader(shader)
    if (!this.gl.getShaderParameter(shader, this.gl.COMPILE_STATUS)) {
      console.error('[HydraWebGL] Shader compile error:', this.gl.getShaderInfoLog(shader))
      this.gl.deleteShader(shader)
      return null
    }
    return shader
  }

  private createProgram(vsSource: string, fsSource: string): WebGLProgram | null {
    if (!this.gl) return null
    const vs = this.createShader(this.gl.VERTEX_SHADER, vsSource)
    const fs = this.createShader(this.gl.FRAGMENT_SHADER, fsSource)
    if (!vs || !fs) return null

    const prog = this.gl.createProgram()
    if (!prog) return null
    this.gl.attachShader(prog, vs)
    this.gl.attachShader(prog, fs)
    this.gl.linkProgram(prog)

    if (!this.gl.getProgramParameter(prog, this.gl.LINK_STATUS)) {
      console.error('[HydraWebGL] Program link error:', this.gl.getProgramInfoLog(prog))
      this.gl.deleteProgram(prog)
      return null
    }

    // Shaders can be detached once linked
    this.gl.deleteShader(vs)
    this.gl.deleteShader(fs)
    return prog
  }

  private setupShaders() {
    const gl = this.gl
    if (!gl) return

    this.programRgba = this.createProgram(VERTEX_SHADER_SOURCE, FRAGMENT_SHADER_RGBA)
    this.programYuv = this.createProgram(VERTEX_SHADER_SOURCE, FRAGMENT_SHADER_YUV)

    // Full screen quad (-1 to 1)
    const positions = new Float32Array([
      -1.0, -1.0,
       1.0, -1.0,
      -1.0,  1.0,
       1.0,  1.0
    ])
    this.positionBuffer = gl.createBuffer()
    gl.bindBuffer(gl.ARRAY_BUFFER, this.positionBuffer)
    gl.bufferData(gl.ARRAY_BUFFER, positions, gl.STATIC_DRAW)

    // Standard Texture coords (0 to 1, flipped vertically for webgl coordinates)
    const texCoords = new Float32Array([
      0.0, 1.0,
      1.0, 1.0,
      0.0, 0.0,
      1.0, 0.0
    ])
    this.texCoordBuffer = gl.createBuffer()
    gl.bindBuffer(gl.ARRAY_BUFFER, this.texCoordBuffer)
    gl.bufferData(gl.ARRAY_BUFFER, texCoords, gl.STATIC_DRAW)

    // Create RGBA texture
    this.textureRgba = this.createTexture()

    // Create 3 YUV textures
    this.textureY = this.createTexture()
    this.textureU = this.createTexture()
    this.textureV = this.createTexture()
  }

  private createTexture(): WebGLTexture | null {
    const gl = this.gl
    if (!gl) return null
    const tex = gl.createTexture()
    gl.bindTexture(gl.TEXTURE_2D, tex)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR)
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR)
    return tex
  }

  public getEngine(): RenderEngine {
    return this.engine
  }

  /**
   * Render VideoFrame directly from WebCodecs using GPU zero-copy upload.
   */
  public renderVideoFrame(frame: any) {
    if (this.isContextLost) return

    const width = frame.displayWidth || frame.codedWidth
    const height = frame.displayHeight || frame.codedHeight

    if (this.canvas.width !== width || this.canvas.height !== height) {
      this.canvas.width = width
      this.canvas.height = height
    }

    if (this.gl && this.programRgba && this.textureRgba) {
      const gl = this.gl
      gl.viewport(0, 0, width, height)
      gl.useProgram(this.programRgba)

      // Bind positions
      const aPos = gl.getAttribLocation(this.programRgba, 'a_position')
      gl.enableVertexAttribArray(aPos)
      gl.bindBuffer(gl.ARRAY_BUFFER, this.positionBuffer)
      gl.vertexAttribPointer(aPos, 2, gl.FLOAT, false, 0, 0)

      // Bind tex coords
      const aTex = gl.getAttribLocation(this.programRgba, 'a_texCoord')
      gl.enableVertexAttribArray(aTex)
      gl.bindBuffer(gl.ARRAY_BUFFER, this.texCoordBuffer)
      gl.vertexAttribPointer(aTex, 2, gl.FLOAT, false, 0, 0)

      // Upload VideoFrame directly to GPU texture (zero CPU copies)
      gl.activeTexture(gl.TEXTURE0)
      gl.bindTexture(gl.TEXTURE_2D, this.textureRgba)
      try {
        gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA, gl.UNSIGNED_BYTE, frame)
        gl.uniform1i(gl.getUniformLocation(this.programRgba, 'u_image'), 0)
        gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4)
      } catch (err) {
        // In case browser WebGL cannot direct upload VideoFrame, fallback to drawImage
        if (!this.ctx2d) {
          this.ctx2d = this.canvas.getContext('2d')
        }
        this.ctx2d?.drawImage(frame, 0, 0, width, height)
      }
    } else {
      // 2D Canvas Fallback
      if (!this.ctx2d) {
        this.ctx2d = this.canvas.getContext('2d')
      }
      this.ctx2d?.drawImage(frame, 0, 0, width, height)
    }
  }

  /**
   * Render raw planar YUV420p data from WASM software decoder.
   * Y plane: width x height
   * U plane: (width / 2) x (height / 2)
   * V plane: (width / 2) x (height / 2)
   */
  public renderYUV(width: number, height: number, yData: Uint8Array, uData: Uint8Array, vData: Uint8Array) {
    if (this.isContextLost) return

    if (this.canvas.width !== width || this.canvas.height !== height) {
      this.canvas.width = width
      this.canvas.height = height
    }

    if (this.gl && this.programYuv && this.textureY && this.textureU && this.textureV) {
      const gl = this.gl
      gl.viewport(0, 0, width, height)
      gl.useProgram(this.programYuv)

      // Bind positions
      const aPos = gl.getAttribLocation(this.programYuv, 'a_position')
      gl.enableVertexAttribArray(aPos)
      gl.bindBuffer(gl.ARRAY_BUFFER, this.positionBuffer)
      gl.vertexAttribPointer(aPos, 2, gl.FLOAT, false, 0, 0)

      // Bind tex coords
      const aTex = gl.getAttribLocation(this.programYuv, 'a_texCoord')
      gl.enableVertexAttribArray(aTex)
      gl.bindBuffer(gl.ARRAY_BUFFER, this.texCoordBuffer)
      gl.vertexAttribPointer(aTex, 2, gl.FLOAT, false, 0, 0)

      // Upload Y Plane
      gl.activeTexture(gl.TEXTURE0)
      gl.bindTexture(gl.TEXTURE_2D, this.textureY)
      gl.pixelStorei(gl.UNPACK_ALIGNMENT, 1)
      gl.texImage2D(gl.TEXTURE_2D, 0, gl.LUMINANCE, width, height, 0, gl.LUMINANCE, gl.UNSIGNED_BYTE, yData)
      gl.uniform1i(gl.getUniformLocation(this.programYuv, 'u_textureY'), 0)

      // Upload U Plane
      gl.activeTexture(gl.TEXTURE1)
      gl.bindTexture(gl.TEXTURE_2D, this.textureU)
      gl.texImage2D(gl.TEXTURE_2D, 0, gl.LUMINANCE, width / 2, height / 2, 0, gl.LUMINANCE, gl.UNSIGNED_BYTE, uData)
      gl.uniform1i(gl.getUniformLocation(this.programYuv, 'u_textureU'), 1)

      // Upload V Plane
      gl.activeTexture(gl.TEXTURE2)
      gl.bindTexture(gl.TEXTURE_2D, this.textureV)
      gl.texImage2D(gl.TEXTURE_2D, 0, gl.LUMINANCE, width / 2, height / 2, 0, gl.LUMINANCE, gl.UNSIGNED_BYTE, vData)
      gl.uniform1i(gl.getUniformLocation(this.programYuv, 'u_textureV'), 2)

      // Render on GPU
      gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4)
    }
  }

  private cleanupGL() {
    if (this.gl) {
      if (this.programRgba) this.gl.deleteProgram(this.programRgba)
      if (this.programYuv) this.gl.deleteProgram(this.programYuv)
      if (this.positionBuffer) this.gl.deleteBuffer(this.positionBuffer)
      if (this.texCoordBuffer) this.gl.deleteBuffer(this.texCoordBuffer)
      if (this.textureRgba) this.gl.deleteTexture(this.textureRgba)
      if (this.textureY) this.gl.deleteTexture(this.textureY)
      if (this.textureU) this.gl.deleteTexture(this.textureU)
      if (this.textureV) this.gl.deleteTexture(this.textureV)
    }
    this.programRgba = null
    this.programYuv = null
    this.positionBuffer = null
    this.texCoordBuffer = null
    this.textureRgba = null
    this.textureY = null
    this.textureU = null
    this.textureV = null
  }

  public destroy() {
    this.canvas.removeEventListener('webglcontextlost', this.boundContextLostHandler)
    this.canvas.removeEventListener('webglcontextrestored', this.boundContextRestoredHandler)
    this.cleanupGL()
    this.gl = null
    this.ctx2d = null
  }
}
