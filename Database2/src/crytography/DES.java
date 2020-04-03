package crytography;

public class DES {
	// We shall use the given functions IP(), F(), FP()
	// We also define the function xor() and its helpers: bitOf() and charOf()
	public static String DESEncrypt(String pt, String[] keys) {
		// pt is a binary strings of length 64
		// keys is an array of, each index contains a binary string of length 56
		pt = IP(pt);
		String oldLeft = pt.substring(0, 32);
		String oldRight = pt.substring(32, 64);
		String newLeft = "";
		String newRight = "";
		for (int i = 0; i < 16; i++) {
			newLeft = oldRight;
			newRight = xor(oldLeft, F(oldRight,keys[i]));
			oldLeft = newLeft;
			oldRight = newRight;
		}
		return FP(pt);
	}

	public static String xor(String a, String b) {
		String out = "";
		for (int i = 0; i < a.length(); i++) {
			out += charOf(bitOf(a.charAt(i)) ^ bitOf(b.charAt(i)));
		}
		return out;
	}

	public static String IP(String s) {
		// apply initial permuation
		return s;
	}

	public static String F(String s, String k) {
		// apply F on s using k
		return s;
	}

	public static String FP(String s) {
		// apply final permuation
		return s;
	}

	public static boolean bitOf(char in) {
		return (in == '1');
	}

	public static char charOf(boolean in) {
		return (in) ? '1' : '0';
	}

}
