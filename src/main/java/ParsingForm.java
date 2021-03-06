import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;

public class ParsingForm {
  private static Schema readSchemaFile(String filepath) {
    try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
      StringBuilder sb = new StringBuilder();
      String line = br.readLine();
      while (line != null) {
        sb.append(line);
        sb.append(System.lineSeparator());
        line = br.readLine();
      }
      String schemaString = sb.toString();

      return new Schema.Parser().parse(schemaString);
    } catch (IOException err) {
      return null;
    }
  }

  /**
   * Read schema file from args, generate parsing form, save to file.
   *
   * @param args single member is schema filepath
   */
  public static void main( String[] args ) {
    if (args.length < 1 || args.length > 2) {
      throw new IllegalArgumentException("invalid arguments: must pass avsc filepath");
    }
    String filepath = args[0];
    String[] parts = filepath.split(File.separator);
    String filename = parts[parts.length - 1];

    if (!filepath.endsWith(".avsc")) {
      throw new IllegalArgumentException("bad filepath given");
    }

    String outFilepath;

    if (args.length == 2) {
      outFilepath = args[1] + File.separator + filename.substring(0, filename.length() - 5);
    } else {
      outFilepath = filepath.substring(0, filepath.length() - 5);
    }
    outFilepath += "_parsing-form.avsc";

    Schema schema = readSchemaFile(filepath);
    String parsingForm = SchemaNormalization.toParsingForm(schema);

    byte[] fp;
    long fp64;
    try {
      byte[] bytes = parsingForm.getBytes("UTF-8");
      fp64 = SchemaNormalization.fingerprint64(bytes);
      fp = SchemaNormalization.fingerprint("CRC-64-AVRO", bytes);
    } catch (NoSuchAlgorithmException err) {
      throw new RuntimeException(err);
    } catch (UnsupportedEncodingException err) {
      throw new RuntimeException(err);
    }
    StringBuilder sb = new StringBuilder();
    for (byte b : fp) {
      sb.append(String.format("%02X", b));
    }

    System.out.printf("Fingerprint is: %d (long) %s (hex)\n", fp64, sb.toString());
    System.out.printf("Writing parsing form to \"%s\"\n", outFilepath);

    try (PrintWriter out = new PrintWriter(outFilepath)) {
      out.print(parsingForm);

    } catch (FileNotFoundException err) {
      err.printStackTrace();
    }
  }
}
