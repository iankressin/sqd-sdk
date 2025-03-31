export function isHex(value: unknown): value is string {
    return typeof value === 'string' && value.length % 2 === 0 && /^(0x)?[a-f\d]*$/i.test(value)
}

const HEX = Array.from({length: 256}, (_, i) => i.toString(16).padStart(2, '0'))

export function toHex(value: Uint8Array) {
    const hexOctets = new Array(value.length + 1)
    hexOctets[0] = '0x'
    for (let i = 0; i < value.length; i++) {
        hexOctets[i + 1] = HEX[value[i]]
    }
    return hexOctets.join('')
}

export function decodeHex(value: string): Uint8Array {
    if (value.length % 2 !== 0) {
        throw new Error(`Invalid bytes "${value}"`)
    }
    const bytes = new Uint8Array(value.length / 2)
    let pos = value.startsWith('0x') ? 2 : 0
    for (let i = 0; i < bytes.length; i++) {
        const left = toHexCode(value.charCodeAt(pos++))
        const right = toHexCode(value.charCodeAt(pos++))
        if (left === undefined || right === undefined) {
            throw new Error(`Invalid bytes "${value}"`)
        }
        bytes[i] = left * 16 + right
    }
    return bytes
}

const HEX_CODES = {
    zero: '0'.charCodeAt(0),
    nine: '9'.charCodeAt(0),
    A: 'A'.charCodeAt(0),
    F: 'F'.charCodeAt(0),
    a: 'a'.charCodeAt(0),
    f: 'f'.charCodeAt(0),
}

function toHexCode(value: number): number | undefined {
    if (value >= HEX_CODES.zero && value <= HEX_CODES.nine) return value - HEX_CODES.zero
    if (value >= HEX_CODES.A && value <= HEX_CODES.F) return value - HEX_CODES.A + 10
    if (value >= HEX_CODES.a && value <= HEX_CODES.f) return value - HEX_CODES.a + 10
    return undefined
}
