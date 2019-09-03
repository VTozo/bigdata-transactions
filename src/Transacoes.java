import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.log4j.BasicConfigurator;

public class Transacoes {

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();
        Configuration c = new Configuration();

        // arquivo de entrada
        Path input = new Path("in\\transactions.csv");

        // arquivo de saida
        Path output = new Path("output\\transactions-" + System.currentTimeMillis());

        // criacao do job e seu nome
        Job j = Job.getInstance(c, "transacoes");

        // Registrar as classes
        j.setJarByClass(Transacoes.class);
        j.setMapperClass(TransacoesMapper.class);
        j.setReducerClass(TransacoesReducer.class);

        // Definição dos tipos de saída
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(TransacoesWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(TransacoesWritable.class);

        // Passando para o job os arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

}
