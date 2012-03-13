package org.I0Itec.zkclient;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;

/**
 * Description: ZooKeeper-Authentication Test
 * @author   nileader / nileader@gmail.com
 * @Date	 Feb 12, 2012
 */
public class DemoAuth implements Watcher {

	final static String SERVER_LIST = "127.0.0.1:4711";
	
	final static String PATH = "/yinshi_auth_test";
	final static String PATH_DEL = "/yinshi_auth_test/will_be_del";

	final static String authentication_type = "digest";

	final static String correctAuthentication = "taokeeper:true";
	final static String badAuthentication = "taokeeper:errorCode";

	static ZkClient zkClient = null;

	public static void main( String[] args ) throws Exception {

		List< ACL > acls = new ArrayList< ACL >( 1 );
		for ( ACL ids_acl : Ids.CREATOR_ALL_ACL ) {
			acls.add( ids_acl );
		}

		try {
			zkClient = new ZkClient( SERVER_LIST, 50000);
			zkClient.addAuthInfo( authentication_type, correctAuthentication.getBytes() );
		} catch ( Exception e ) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			zkClient.createPersistent( PATH, acls, "init content" );
			System.out.println( "使用授权key：" + correctAuthentication + "创建节点：" + PATH + ", 初始内容是: init content" );
		} catch ( Exception e ) {
			e.printStackTrace();
		}
		try {
			zkClient.createPersistent( PATH_DEL, acls, "待删节点" );
			System.out.println( "使用授权key：" + correctAuthentication + "创建节点：" + PATH_DEL + ", 初始内容是: init content" );
		} catch ( Exception e ) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// 获取数据
		getDataByNoAuthentication();
		getDataByBadAuthentication();
		getDataByCorrectAuthentication();

		// 更新数据
		updateDataByNoAuthentication();
		updateDataByBadAuthentication();
		updateDataByCorrectAuthentication();

		// 获取数据
		getDataByNoAuthentication();
		getDataByBadAuthentication();
		getDataByCorrectAuthentication();

		//删除数据
		deleteNodeByBadAuthentication();
		deleteNodeByNoAuthentication();
		deleteNodeByCorrectAuthentication();

		deleteParent();
		
		zkClient.close();
	}

	/** 获取数据：采用错误的密码 */
	static void getDataByBadAuthentication() {
		String prefix = "[使用错误的授权信息]";
		try {
			System.out.println( prefix + "获取数据：" + PATH );
			zkClient = new ZkClient( SERVER_LIST, 50000);
			zkClient.addAuthInfo( authentication_type, badAuthentication.getBytes() );
			System.out.println( prefix + "成功获取数据：" + zkClient.readData( PATH ) );
		} catch ( Exception e ) {
			System.err.println( prefix + "获取数据失败，原因：" + e.getMessage() );
		}
	}

	/** 获取数据：不采用密码 */
	static void getDataByNoAuthentication() {
		String prefix = "[不使用任何授权信息]";
		try {
			System.out.println( prefix + "获取数据：" + PATH );
			zkClient = new ZkClient( SERVER_LIST, 50000);
			System.out.println( prefix + "成功获取数据：" + zkClient.readData( PATH ) );
		} catch ( Exception e ) {
			System.err.println( prefix + "获取数据失败，原因：" + e.getMessage() );
		}
	}

	/** 采用正确的密码 */
	static void getDataByCorrectAuthentication() {
		String prefix = "[使用正确的授权信息]";
		try {
			System.out.println( prefix + "获取数据：" + PATH );
			zkClient = new ZkClient( SERVER_LIST, 50000);
			zkClient.addAuthInfo( authentication_type, correctAuthentication.getBytes() );
			System.out.println( prefix + "成功获取数据：" + zkClient.readData( PATH ) );
		} catch ( Exception e ) {
			System.out.println( prefix + "获取数据失败，原因：" + e.getMessage() );
		}
	}

	/**
	 * 更新数据：不采用密码
	 */
	static void updateDataByNoAuthentication() {
		
		String prefix = "[不使用任何授权信息]";
		
		System.out.println( prefix + "更新数据： " + PATH );
		try {
			zkClient = new ZkClient( SERVER_LIST, 50000);
			if( zkClient.exists( PATH ) ){
				zkClient.writeData( PATH, prefix );
				System.out.println( prefix + "更新成功" );
			}
		} catch ( Exception e ) {
			System.err.println( prefix + "更新失败，原因是：" + e.getMessage() );
		}
	}

	/**
	 * 更新数据：采用错误的密码
	 */
	static void updateDataByBadAuthentication() {
		
		String prefix = "[使用错误的授权信息]";
		
		System.out.println( prefix + "更新数据：" + PATH );
		try {
			zkClient = new ZkClient( SERVER_LIST, 50000);
			zkClient.addAuthInfo( authentication_type, badAuthentication.getBytes() );
			if( zkClient.exists( PATH ) ){
				zkClient.writeData( PATH, prefix );
				System.out.println( prefix + "更新成功" );
			}
		} catch ( Exception e ) {
			System.err.println( prefix + "更新失败，原因是：" + e.getMessage() );
		}
	}

	/**
	 * 更新数据：采用正确的密码
	 */
	static void updateDataByCorrectAuthentication() {
		
		String prefix = "[使用正确的授权信息]";
		
		System.out.println( prefix + "更新数据：" + PATH );
		try {
			zkClient = new ZkClient( SERVER_LIST, 50000);
			zkClient.addAuthInfo( authentication_type, correctAuthentication.getBytes() );
			if( zkClient.exists( PATH ) ){
				zkClient.writeData( PATH, prefix );
				System.out.println( prefix + "更新成功" );
			}
		} catch ( Exception e ) {
			System.err.println( prefix + "更新失败，原因是：" + e.getMessage() );
		}
	}

	
	/**
	 * 不使用密码 删除节点
	 */
	static void deleteNodeByNoAuthentication() throws Exception {
		
		String prefix = "[不使用任何授权信息]";
		
		try {
			System.out.println( prefix + "删除节点：" + PATH_DEL );
			zkClient = new ZkClient( SERVER_LIST, 50000);
			if( zkClient.exists( PATH_DEL ) ){
				zkClient.delete( PATH_DEL );
				System.out.println( prefix + "删除成功" );
			}
		} catch ( Exception e ) {
			System.err.println( prefix + "删除失败，原因是：" + e.getMessage() );
		}
	}
	
	
	
	/**
	 * 采用错误的密码删除节点
	 */
	static void deleteNodeByBadAuthentication() throws Exception {
		
		String prefix = "[使用错误的授权信息]";
		
		try {
			System.out.println( prefix + "删除节点：" + PATH_DEL );
			zkClient = new ZkClient( SERVER_LIST, 50000);
			zkClient.addAuthInfo( authentication_type, badAuthentication.getBytes() );
			if( zkClient.exists( PATH_DEL ) ){
				zkClient.delete( PATH_DEL );
				System.out.println( prefix + "删除成功" );
			}
		} catch ( Exception e ) {
			System.err.println( prefix + "删除失败，原因是：" + e.getMessage() );
		}
	}



	/**
	 * 使用正确的密码删除节点
	 */
	static void deleteNodeByCorrectAuthentication() throws Exception {
		
		String prefix = "[使用正确的授权信息]";
		
		try {
			System.out.println( prefix + "删除节点：" + PATH_DEL );
			zkClient = new ZkClient( SERVER_LIST, 50000);
			zkClient.addAuthInfo( authentication_type, correctAuthentication.getBytes() );
			if( zkClient.exists( PATH_DEL ) ){
				zkClient.delete( PATH_DEL );
				System.out.println( prefix + "删除成功" );
			}
		} catch ( Exception e ) {
			System.out.println( prefix + "删除失败，原因是：" + e.getMessage() );
		}
	}
	
	
	
	/**
	 * 使用正确的密码删除节点
	 */
	static void deleteParent() throws Exception {
		try {
			zkClient = new ZkClient( SERVER_LIST, 50000);
			zkClient.addAuthInfo( authentication_type, correctAuthentication.getBytes() );
			if( zkClient.exists( PATH ) ){
				zkClient.delete( PATH );
			}
		} catch ( Exception e ) {
			e.printStackTrace();
		}
	}

	@Override
	public void process( WatchedEvent event ) {
		// TODO Auto-generated method stub
		
	}
	
	
	
	

}
