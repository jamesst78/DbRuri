package crytography;

public class Vigenere {
	public static void main(String[] args) {
		System.out.println(decrypt(encrypt("my name is", "lemon"), "lemon"));
	}

	public static int alphaToNum(char i) {
		if (i >= 97 && i <= 122)
			return i - 97;
		else
			return -1;
	}

	public static char numToAlpha(int i) {
		if (i >= 0 && i <= 25)
			return (char) (i + 97);
		else
			return '$';
	}

	public static String encrypt(String pt, String key) {
		String s = "";
		for (int i = 0; i < pt.length(); i++) {
			if (pt.charAt(i) == ' ')
				s += ' ';
			else {
				char c = numToAlpha((alphaToNum(pt.charAt(i)) + alphaToNum(key.charAt(i % key.length()))) % 26);
				s += c;
			}
		}
		return s;
	}

	public static String decrypt(String pt, String key) {
		String s = "";
		for (int i = 0; i < pt.length(); i++) {
			if (pt.charAt(i) == ' ')
				s += ' ';
			else {
				char c = numToAlpha((alphaToNum(pt.charAt(i)) + 26 - alphaToNum(key.charAt(i % key.length()))) % 26);
				s += c;
			}
		}
		return s;
	}
}
