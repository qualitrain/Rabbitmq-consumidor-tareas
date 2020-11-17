package qtx.consumo;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerShutdownSignalCallback;
import com.rabbitmq.client.DeliverCallback;

public class ConsumidorTarea {
	private String nombreColaTareas;
	private String hostRabbitMQ;
	private Connection conexion;
	private Channel canal;
	private String consumerTag;
	private static final int MAX_CANT_MENSAJES = 1;
	
	public ConsumidorTarea(String nombreColaTareas, String hostRabbitMQ) {
		super();
		this.nombreColaTareas = nombreColaTareas;
		this.hostRabbitMQ = hostRabbitMQ;
	}

	public ConsumidorTarea() {
		super();
		this.nombreColaTareas = "colaTareas01";
		this.hostRabbitMQ = "localhost";
	}

	public void consumirTareas() {
		ConnectionFactory fabricaConexiones = new ConnectionFactory();
		fabricaConexiones.setHost(this.hostRabbitMQ);
		try {
			this.conexion = fabricaConexiones.newConnection();
			this.canal = this.conexion.createChannel();
			
			//*** queueDeclare(queue, durable, exclusive, autoDelete, arguments)
			canal.queueDeclare(this.nombreColaTareas, true, false, false, null);
			canal.basicQos(MAX_CANT_MENSAJES);
			
			DeliverCallback procesadorMensajes = getProcesadorMensajes(canal);
			CancelCallback procesadorCancelacion = getProcesadorCancelacion(canal,conexion);
			
			ConsumerShutdownSignalCallback finalizador = getProcesadorFin(this.canal, this.conexion);
			
			this.consumerTag = canal.basicConsume(this.nombreColaTareas, false, 
					                     procesadorMensajes, 
					                     procesadorCancelacion, 
					                     finalizador
					                     );
			System.out.println("Consumer Tag:[" + consumerTag + "]");
		}
		catch (IOException | TimeoutException e) {
			e.printStackTrace();
		}
		finally {
			System.out.println("Finalmente...(este hilo terminó su ejecución, pero hay otro consumiendo tareas)");
		}
	}
	private CancelCallback getProcesadorCancelacion(Channel canal, Connection conexion) {
		return consumerTag-> {
			System.out.println("Se ha cancelado este consumidor " + consumerTag);
			try {
				canal.close();
				conexion.close();
			} catch (TimeoutException e) {
				e.printStackTrace();
			}
		};
	} 

	private DeliverCallback getProcesadorMensajes(Channel canal) {
		DeliverCallback callback = (consumerTag, objMensaje) -> {
			String contenido = new String(objMensaje.getBody(),"UTF-8");
			System.out.println("Mensaje recibido: [" + contenido + "] consumerTag: [" + consumerTag + "]");
			System.out.println("Hilo:" + Thread.currentThread().getId() + ", Hilos (aprox):" + Thread.activeCount());
			try {
				procesarMensaje(contenido);
			}
			finally {
				System.out.println("Mensaje procesado");
//**			basicAck‹(deliveryTag, boolean multiple)
				canal.basicAck(objMensaje.getEnvelope().getDeliveryTag(), false);
			}
		};
		return callback;
	}

	private void procesarMensaje(String contenido) {
		System.out.println("Procesando mensaje:[" + contenido + "]");
		try {
			Thread.sleep(1000 * ((contenido.length() % 5) + 1) );
		} catch (InterruptedException e) {
			e.printStackTrace();
		} 
	}
	
	private ConsumerShutdownSignalCallback getProcesadorFin(Channel canal, Connection conexion) {
		ConsumerShutdownSignalCallback finalizador =
				(consumerTag,sig)->{
					System.out.println("****************** Shutdown ******************");
					try { 
						if(canal.isOpen())
						   canal.close();
					}catch (TimeoutException | IOException e) {
						e.printStackTrace();
					}
					try {
						if(conexion.isOpen())
						   conexion.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					System.exit(0);
				};
		return finalizador;
	}
}
