// @ts-ignore
import { decode } from "base-64";

export function base64Decode(encodedString: string): string | null {
    try {
        return decode(encodedString);
    } catch (error) {
        console.error('Base64 decoding error:', error);
        return null;
    }
}