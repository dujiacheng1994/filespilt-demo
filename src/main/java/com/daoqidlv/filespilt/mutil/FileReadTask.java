package com.daoqidlv.filespilt.mutil;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.BlockingQueue;

import com.daoqidlv.filespilt.Constants;

/**
 * 文件读取任务
 * @author Administrator
 *
 */

public class FileReadTask extends Thread{
	/**
	 * 任务序号
	 */
	private int taskSeq;
	/**
	 * 任务名称
	 */
	private String taskName;
	/**
	 * 读取开始的点
	 */
	private int beginFilePointer;
	/**
	 * 读取结束的点
	 */
	private int endFilePointer;
	/**
	 * 待读取的总大小
	 */	
	private int toReadSize;
	/**
	 * 源文件全路径
	 */
	private String originFileFullName;
	/**
	 * 实际读取的数据大小
	 */
	private int readedSize;
	/**
	 * 一次读取的字节数
	 */
	private int readSizeOneTime;
	/**
	 * 用于交换子文件内容的阻塞队列
	 */
	private BlockingQueue<FileLine> queue;

	public FileReadTask(int taskSeq, int beginFilePointer, int endFilePointer, String originFileFullName, BlockingQueue<FileLine> queue) {
		this.taskSeq = taskSeq;
		this.beginFilePointer = beginFilePointer;
		this.endFilePointer = endFilePointer;
		this.originFileFullName = originFileFullName;
		this.queue = queue;
		this.readSizeOneTime = 10*1024*1024;	// 一次最少读取10M
		this.taskName = "FileReadTask_" + this.taskSeq;
		this.readedSize = 0;
		this.toReadSize = this.endFilePointer - this.beginFilePointer + 1;	// 读取闭区间
	}
	
	/**
	 * 从指定文件目录读取文件内容，并将每行内容转换为FileLine对象，写入queue中。
	 * 行结束标志：当读取到/n 或者 /r字节时
	 */
	@Override
	public void run() {
		try {
			@SuppressWarnings("resource")
			FileInputStream originFile = new FileInputStream(this.originFileFullName);
			int index = this.beginFilePointer;
			int totalSpiltedSize = 0;
			MappedByteBuffer inputBuffer = null;

			// 正常应该是<=
			// 不过如果是整行，则改行最后两个字符是/r/n，所以index小于this.endFilePointer才需要进入，如果只是1个字符，必然是/r，就不用读了
			// index是起指针，并未已读
			for(; index < this.endFilePointer;) {
				//System.err.println(Thread.currentThread().getName()+": index = "+index);
				totalSpiltedSize = Math.min(this.readSizeOneTime, this.endFilePointer-index+1); // 1次处理的文件大小，也是mmap大小，要么是10M，要么是末尾少于10M的内容
				inputBuffer = originFile.getChannel().map(FileChannel.MapMode.READ_ONLY, index, totalSpiltedSize);	// 尾指针是 index + totalSpl - 1， 记得要减1
				try {
					index = spiltFileLine(inputBuffer, index, totalSpiltedSize);	// 把这块mmap分行读然后扔到Line队列，同时更新index，这里会涉及到内存拷贝？
				} catch (Exception e) {
					e.printStackTrace();
					return;
				}
				
			}
			
			// 对于最后一个task，可能最后一行并不含/r/n符号，还存在剩余未拆分的文件行内容，直接当着单独的行写入
			// 实际这个是spiltFileLine方法里的
			if(this.endFilePointer - index >= 0) {	// 如果还没读完，包括index == end 也是没读完！
				int fileLineSize = this.endFilePointer - index ;
				byte[] fileLineCache = new byte[fileLineSize];
				inputBuffer.get(fileLineCache, 0, fileLineSize);
				FileLine fileLine = new FileLine(fileLineSize, fileLineCache);
				//将fileLine对象放入queue中
				try {
					this.queue.put(fileLine);			// 使用阻塞方式写入消息，这里是单线程！，一旦队列满则不再向里面写入消息，确保在消费者处理过慢的情况下，不会出现oom
					this.readedSize += fileLineSize;	// 更新本线程已读取字节
					//System.out.println(Thread.currentThread().getName()+" put one fileLine to queue: "+fileLine);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * 从字节缓存中拆分出每一行对应的字节数据，并放入队列
	 * @param inputBuffer 目标字节缓存
	 * @param index 进行拆分的起始字节位置，拆分完之后停在未读的第1个位置
	 * @param totalSpiltedSize 总共需要进行拆分的字节数
	 * @return index 拆分处理后，最新的待处理字节的起始位置
	 * @throws Exception 
	 * @TODO 可以直接使用RandomAccessFile.readLine()方法
	 */
	int spiltFileLine(MappedByteBuffer inputBuffer, int index, int totalSpiltedSize) throws Exception {
		int subIndex = index;	// 标记该mmap最初的index，绝对起始点
		boolean newLineFlag = false;
		for(int i = 0; i < totalSpiltedSize - 1; i++) {		// 这里到尾部-1就停了，因为/r/n是两个，如果是/n，那就要去掉-1这个东西！
			if(Constants.ENTER_CHAR_ASCII == inputBuffer.get(i) && Constants.NEW_LINE_CHAR_ASCII == inputBuffer.get(i+1)) {			//遇到\r\n
				newLineFlag = true;
				i++; //将/r后面的/n字节也包含到行中

				int fileLineSize = subIndex+i - index + 1; // [index,subIndex+i]，i为绝对位置指针
				byte[] fileLineCache = new byte[fileLineSize];
				inputBuffer.get(fileLineCache, 0, fileLineSize); // 创建字节数组并从mmap读取内容存入
				FileLine fileLine = new FileLine(fileLineSize, fileLineCache);

				try {
					this.queue.put(fileLine);				// 将fileLine对象放入queue中
					this.readedSize += fileLineSize;
					// System.out.println(Thread.currentThread().getName()+" put one fileLine to queue: "+fileLine);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				index += fileLineSize;	// index更新，此时仍位于未读第1位
			}
		}
		if(!newLineFlag) {	// 如果这个块压根没找到换行符，就认为是最后一行，直接把它作为fileLine
			if(totalSpiltedSize < this.readSizeOneTime) {		// todo: 也有可能等于，此时表示???
				byte[] tempBytes = new byte[totalSpiltedSize];
				inputBuffer.get(tempBytes, 0, totalSpiltedSize);
				FileLine fileLine = new FileLine(totalSpiltedSize, tempBytes);
				try {
					this.queue.put(fileLine);
					this.readedSize += totalSpiltedSize;
					// System.out.println(Thread.currentThread().getName()+" put one fileLine to queue: "+fileLine);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				index += totalSpiltedSize;
			} else {  	//文件行内容过大，无法按行读取，需要调节readSizeOneTime参数
				System.err.println("index="+index+",totalSpiltedSize="+totalSpiltedSize);
				System.err.println("this content has no new line, and its length has greater than "+this.readSizeOneTime);	
				throw new Exception("this content has no new line, and its length has greater than "+this.readSizeOneTime) ;				
			}
		}
		return index;
	}
	
	private void writeSubFile(MappedByteBuffer inputBuffer, int index, int totalSpiltedSize) {
		String subFileName = "D:\\temp\\error.txt";
		byte[] tempBytes = new byte[totalSpiltedSize];
		inputBuffer.get(tempBytes, 0, totalSpiltedSize);
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(subFileName);
			fos.write(tempBytes);
			fos.flush();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public int getTaskSeq() {
		return taskSeq;
	}

	public void setTaskSeq(int taskSeq) {
		this.taskSeq = taskSeq;
	}

	public int getBeginFilePointer() {
		return beginFilePointer;
	}

	public void setBeginFilePointer(int beginFilePointer) {
		this.beginFilePointer = beginFilePointer;
	}

	public int getToReadSize() {
		return toReadSize;
	}

	public void setToReadSize(int toReadSize) {
		this.toReadSize = toReadSize;
	}

	public String getoriginFileFullName() {
		return originFileFullName;
	}

	public void setoriginFileFullName(String originFileFullName) {
		this.originFileFullName = originFileFullName;
	}
	
	public int getReadedSize() {
		return readedSize;
	}

	public void setReadedSize(int readedSize) {
		this.readedSize = readedSize;
	}

	public String getTaskName() {
		return taskName;
	}

	public void setTaskName(String taskName) {
		this.taskName = taskName;
	}

	public int getEndFilePointer() {
		return endFilePointer;
	}

	public void setEndFilePointer(int endFilePointer) {
		this.endFilePointer = endFilePointer;
	}

	public int getReadSizeOneTime() {
		return readSizeOneTime;
	}

	public void setReadSizeOneTime(int readSizeOneTime) {
		this.readSizeOneTime = readSizeOneTime;
	}

	@Override
	public String toString() {
		return "FileReadTask [taskSeq=" + taskSeq + ", taskName=" + taskName + ", beginFilePointer=" + beginFilePointer
				+ ", endFilePointer=" + endFilePointer + ", toReadSize=" + toReadSize + ", originFileFullName="
				+ originFileFullName + ", readedSize=" + readedSize + ", readSizeOneTime=" + readSizeOneTime + "]";
	}
}
