package ingest

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

func (r *RTSPIngestor) connectAndDemuxRTSP(ctx context.Context, sess *ingestSession) error {
	u, err := url.Parse(sess.sourceURL)
	if err != nil {
		return fmt.Errorf("invalid RTSP URL: %w", err)
	}

	host := u.Host
	if !strings.Contains(host, ":") {
		host += ":554"
	}

	var username, password string
	if u.User != nil {
		username = u.User.Username()
		password, _ = u.User.Password()
	}

	rawURI := fmt.Sprintf("rtsp://%s%s", u.Host, u.Path)
	if u.RawQuery != "" {
		rawURI += "?" + u.RawQuery
	}
	if rawURI == "" || rawURI == "rtsp://" {
		rawURI = sess.sourceURL
	}

	d := net.Dialer{Timeout: 5 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", host)
	if err != nil {
		return fmt.Errorf("dial TCP %s failed: %w", host, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	sess.mu.Lock()
	sess.conn = conn
	sess.mu.Unlock()

	br := bufio.NewReaderSize(conn, 65536)
	cseq := 1
	var wwwAuthChallenge string

	if err := sendRTSPRequest(conn, "OPTIONS", rawURI, cseq, ""); err != nil {
		return fmt.Errorf("OPTIONS send failed: %w", err)
	}
	optHeaders, _, err := readRTSPMessage(br)
	if err != nil {
		return fmt.Errorf("OPTIONS read failed: %w", err)
	}
	if strings.Contains(optHeaders, "401 Unauthorized") {
		wwwAuthChallenge = extractHeader(optHeaders, "WWW-Authenticate")
	}
	cseq++

	authHeader := buildAuthHeader(wwwAuthChallenge, "DESCRIBE", rawURI, username, password)
	describeExtra := "Accept: application/sdp\r\n"
	if authHeader != "" {
		describeExtra += authHeader + "\r\n"
	}
	if err := sendRTSPRequest(conn, "DESCRIBE", rawURI, cseq, describeExtra); err != nil {
		return fmt.Errorf("DESCRIBE send failed: %w", err)
	}
	descHeaders, descBody, err := readRTSPMessage(br)
	if err != nil {
		return fmt.Errorf("DESCRIBE read failed: %w", err)
	}

	if strings.Contains(descHeaders, "401 Unauthorized") {
		if username == "" {
			return fmt.Errorf("camera requires authentication (401 Unauthorized)")
		}
		wwwAuthChallenge = extractHeader(descHeaders, "WWW-Authenticate")
		authHeader = buildAuthHeader(wwwAuthChallenge, "DESCRIBE", rawURI, username, password)
		cseq++

		describeExtra = "Accept: application/sdp\r\n"
		if authHeader != "" {
			describeExtra += authHeader + "\r\n"
		}
		if err := sendRTSPRequest(conn, "DESCRIBE", rawURI, cseq, describeExtra); err != nil {
			return fmt.Errorf("DESCRIBE auth send failed: %w", err)
		}
		descHeaders, descBody, err = readRTSPMessage(br)
		if err != nil {
			return fmt.Errorf("DESCRIBE auth read failed: %w", err)
		}
		if strings.Contains(descHeaders, "401 Unauthorized") {
			return fmt.Errorf("RTSP authentication failed (401)")
		}
	}
	if !strings.Contains(descHeaders, "200 OK") {
		return fmt.Errorf("DESCRIBE error: %s", firstStatusLine(descHeaders))
	}
	cseq++

	contentBase := extractHeaderRaw(descHeaders, "Content-Base")
	setupURL := extractVideoTrackURL(rawURI, contentBase, string(descBody))
	setupAuth := buildAuthHeader(wwwAuthChallenge, "SETUP", setupURL, username, password)
	setupExtra := "Transport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n"
	if setupAuth != "" {
		setupExtra += setupAuth + "\r\n"
	}
	if err := sendRTSPRequest(conn, "SETUP", setupURL, cseq, setupExtra); err != nil {
		return fmt.Errorf("SETUP send failed: %w", err)
	}
	setupHeaders, _, err := readRTSPMessage(br)
	if err != nil {
		return fmt.Errorf("SETUP read failed: %w", err)
	}
	if strings.Contains(setupHeaders, "401 Unauthorized") {
		wwwAuthChallenge = extractHeader(setupHeaders, "WWW-Authenticate")
		setupAuth = buildAuthHeader(wwwAuthChallenge, "SETUP", setupURL, username, password)
		cseq++
		setupExtra = "Transport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n"
		if setupAuth != "" {
			setupExtra += setupAuth + "\r\n"
		}
		if err := sendRTSPRequest(conn, "SETUP", setupURL, cseq, setupExtra); err != nil {
			return fmt.Errorf("SETUP auth send failed: %w", err)
		}
		setupHeaders, _, err = readRTSPMessage(br)
		if err != nil {
			return fmt.Errorf("SETUP auth read failed: %w", err)
		}
	}
	if !strings.Contains(setupHeaders, "200 OK") {
		return fmt.Errorf("SETUP error: %s", firstStatusLine(setupHeaders))
	}
	sessionHdr := extractHeader(setupHeaders, "Session")
	cseq++

	playURI := rawURI
	if contentBase != "" {
		playURI = strings.TrimSuffix(contentBase, "/")
		if u.RawQuery != "" && !strings.Contains(playURI, "?") {
			playURI = rawURI
		}
	}
	playAuth := buildAuthHeader(wwwAuthChallenge, "PLAY", playURI, username, password)
	playExtra := ""
	if sessionHdr != "" {
		playExtra += fmt.Sprintf("Session: %s\r\n", sessionHdr)
	}
	if playAuth != "" {
		playExtra += playAuth + "\r\n"
	}
	if err := sendRTSPRequest(conn, "PLAY", playURI, cseq, playExtra); err != nil {
		return fmt.Errorf("PLAY send failed: %w", err)
	}
	playHeaders, _, err := readRTSPMessage(br)
	if err != nil {
		return fmt.Errorf("PLAY read failed: %w", err)
	}
	if strings.Contains(playHeaders, "401 Unauthorized") {
		wwwAuthChallenge = extractHeader(playHeaders, "WWW-Authenticate")
		playAuth = buildAuthHeader(wwwAuthChallenge, "PLAY", playURI, username, password)
		cseq++
		playExtra = ""
		if sessionHdr != "" {
			playExtra += fmt.Sprintf("Session: %s\r\n", sessionHdr)
		}
		if playAuth != "" {
			playExtra += playAuth + "\r\n"
		}
		if err := sendRTSPRequest(conn, "PLAY", playURI, cseq, playExtra); err != nil {
			return fmt.Errorf("PLAY auth send failed: %w", err)
		}
		playHeaders, _, err = readRTSPMessage(br)
		if err != nil {
			return fmt.Errorf("PLAY auth read failed: %w", err)
		}
	}
	if !strings.Contains(playHeaders, "200 OK") {
		return fmt.Errorf("PLAY error: %s", firstStatusLine(playHeaders))
	}
	cseq++
	_ = conn.SetDeadline(time.Time{})

	log.Printf("[HydraStream RTSP] Stream '%s' established active TCP session with %s.", sess.streamID, host)

	r.mu.RLock()
	pub := r.publisher
	r.mu.RUnlock()
	if pub != nil {
		_ = pub.PublishCameraOnline(ctx, "", sess.streamID)
	}

	buf := make([]byte, 65536)
	var lastSeq uint16
	hasSeq := false
	lastKeepalive := time.Now()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if time.Since(lastKeepalive) >= 25*time.Second {
				keepExtra := ""
				if sessionHdr != "" {
					keepExtra = fmt.Sprintf("Session: %s\r\n", sessionHdr)
				}
				_ = sendRTSPRequest(conn, "OPTIONS", rawURI, cseq, keepExtra)
				cseq++
				lastKeepalive = time.Now()
			}
			_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
			prefix, err := br.ReadByte()
			if err != nil {
				return fmt.Errorf("RTP read failed: %w", err)
			}

			if prefix == '$' {
				ch, err := br.ReadByte()
				if err != nil {
					return fmt.Errorf("RTP channel read failed: %w", err)
				}

				var payloadLen uint16
				if err := binary.Read(br, binary.BigEndian, &payloadLen); err != nil {
					return fmt.Errorf("RTP length read failed: %w", err)
				}

				if int(payloadLen) > len(buf) {
					buf = make([]byte, payloadLen)
				}

				if _, err := io.ReadFull(br, buf[:payloadLen]); err != nil {
					return fmt.Errorf("RTP payload read failed: %w", err)
				}

				// Process RTP video packets on interleaved channel 0 (skip RTCP on odd channels)
				if ch == 0 && payloadLen >= 12 {
					atomic.AddUint64(&sess.rtpPacketsTotal, 1)
					atomic.AddUint64(&sess.bytesTotal, uint64(payloadLen))

					seq := binary.BigEndian.Uint16(buf[2:4])
					if hasSeq {
						diff := uint16(seq - lastSeq)
						if diff > 1 && diff < 1000 {
							atomic.AddUint64(&sess.rtpLossTotal, uint64(diff-1))
						}
					}
					lastSeq = seq
					hasSeq = true

					marker := (buf[1] & 0x80) != 0
					if marker {
						atomic.AddUint64(&sess.framesTotal, 1)
					}

					now := time.Now()
					sess.mu.Lock()
					if sess.firstFrameAt.IsZero() {
						sess.firstFrameAt = now
						sess.ttffMs = float64(now.Sub(sess.startedAt).Microseconds()) / 1000.0
					}
					sess.status = "streaming"
					sess.err = nil
					sess.lastFrameTime = now
					sess.mu.Unlock()
				}
			} else {
				// Consume interleaved RTSP response text (e.g., keepalive 200 OK) cleanly
				_ = br.UnreadByte()
				_, _, _ = readRTSPMessage(br)
			}
		}
	}
}

func sendRTSPRequest(w io.Writer, method, uri string, cseq int, extraHeaders string) error {
	cleanHeaders := strings.TrimRight(extraHeaders, "\r\n")
	if cleanHeaders != "" {
		cleanHeaders += "\r\n"
	}
	req := fmt.Sprintf("%s %s RTSP/1.0\r\nCSeq: %d\r\nUser-Agent: HydraStream/1.0\r\n%s\r\n", method, uri, cseq, cleanHeaders)
	_, err := w.Write([]byte(req))
	return err
}

func readRTSPMessage(br *bufio.Reader) (headers string, body []byte, err error) {
	var resp strings.Builder
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return "", nil, err
		}
		resp.WriteString(line)
		if line == "\r\n" || line == "\n" {
			break
		}
	}
	headers = resp.String()

	contentLenStr := extractHeader(headers, "Content-Length")
	if contentLenStr != "" {
		if clen, err := strconv.Atoi(contentLenStr); err == nil && clen > 0 {
			body = make([]byte, clen)
			if _, err := io.ReadFull(br, body); err != nil {
				return headers, nil, err
			}
		}
	}

	return headers, body, nil
}

func firstStatusLine(headers string) string {
	for _, l := range strings.Split(headers, "\r\n") {
		if strings.TrimSpace(l) != "" {
			return strings.TrimSpace(l)
		}
	}
	return "empty response"
}

func extractHeaderRaw(resp, headerName string) string {
	lines := strings.Split(resp, "\r\n")
	prefix := strings.ToLower(headerName) + ":"
	for _, l := range lines {
		if strings.HasPrefix(strings.ToLower(l), prefix) {
			return strings.TrimSpace(l[len(prefix):])
		}
	}
	return ""
}

func extractVideoTrackURL(rawURI, contentBase, sdpBody string) string {
	base := rawURI
	if contentBase != "" {
		base = contentBase
	}
	if sdpBody == "" {
		if strings.Contains(base, "?") {
			return strings.TrimSuffix(base, "/") + "/track1"
		}
		return fmt.Sprintf("%s/track1", strings.TrimSuffix(base, "/"))
	}

	lines := strings.Split(sdpBody, "\n")
	inVideo := false
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "m=video") {
			inVideo = true
			continue
		}
		if inVideo && strings.HasPrefix(line, "m=") {
			break
		}
		if inVideo && strings.HasPrefix(line, "a=control:") {
			track := strings.TrimSpace(strings.TrimPrefix(line, "a=control:"))
			if strings.HasPrefix(track, "rtsp://") {
				return track
			}
			if track == "*" || track == "" {
				return rawURI
			}
			return fmt.Sprintf("%s/%s", strings.TrimSuffix(base, "/"), strings.TrimPrefix(track, "/"))
		}
	}

	return fmt.Sprintf("%s/track1", strings.TrimSuffix(base, "/"))
}

func extractHeader(resp, headerName string) string {
	lines := strings.Split(resp, "\r\n")
	prefix := strings.ToLower(headerName) + ":"
	for _, l := range lines {
		if strings.HasPrefix(strings.ToLower(l), prefix) {
			val := strings.TrimSpace(strings.TrimPrefix(l, l[:len(prefix)]))
			if idx := strings.Index(val, ";"); idx != -1 {
				val = val[:idx]
			}
			return val
		}
	}
	return ""
}

func extractParam(header, param string) string {
	for _, part := range strings.Split(header, ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(strings.ToLower(part), strings.ToLower(param)+"=") {
			val := strings.TrimPrefix(part, part[:len(param)+1])
			return strings.Trim(val, "\"")
		}
		if strings.Contains(strings.ToLower(part), strings.ToLower(param)+"=") {
			idx := strings.Index(strings.ToLower(part), strings.ToLower(param)+"=")
			sub := part[idx+len(param)+1:]
			if comma := strings.Index(sub, ","); comma != -1 {
				sub = sub[:comma]
			}
			return strings.Trim(strings.TrimSpace(sub), "\"")
		}
	}
	return ""
}

func md5Hex(s string) string {
	h := md5.Sum([]byte(s))
	return hex.EncodeToString(h[:])
}

func buildAuthHeader(challenge, method, uri, username, password string) string {
	if username == "" || challenge == "" {
		return ""
	}
	if strings.Contains(strings.ToLower(challenge), "digest") {
		realm := extractParam(challenge, "realm")
		nonce := extractParam(challenge, "nonce")
		if realm != "" && nonce != "" {
			ha1 := md5Hex(fmt.Sprintf("%s:%s:%s", username, realm, password))
			ha2 := md5Hex(fmt.Sprintf("%s:%s", method, uri))
			response := md5Hex(fmt.Sprintf("%s:%s:%s", ha1, nonce, ha2))
			return fmt.Sprintf("Authorization: Digest username=\"%s\", realm=\"%s\", nonce=\"%s\", uri=\"%s\", response=\"%s\"", username, realm, nonce, uri, response)
		}
	}
	if strings.Contains(strings.ToLower(challenge), "basic") {
		cred := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
		return fmt.Sprintf("Authorization: Basic %s", cred)
	}
	return ""
}
