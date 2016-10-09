package cn.mldn.oracle.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import cn.mldn.oracle.dao.IMemberDAO;
import cn.mldn.oracle.dbc.DatabaseConnection;
import cn.mldn.oracle.vo.Member;

public class MemberDAOImpl implements IMemberDAO {
	private Connection conn ;
	private PreparedStatement pstmt ;	// 所有的数据库的操作都通过此接口完成
	public MemberDAOImpl() {
		this.conn = DatabaseConnection.getConnection() ; 
	} 
	@Override
	public boolean doCreate(Member vo) throws Exception {
		String sql = "INSERT INTO member(mid,name,age,phone,birthday,note) VALUES (?,?,?,?,?,?) " ;
		this.pstmt = this.conn.prepareStatement(sql) ;
		this.pstmt.setString(1, vo.getMid());
		this.pstmt.setString(2, vo.getName());
		this.pstmt.setInt(3, vo.getAge());
		this.pstmt.setString(4, vo.getPhone());
		this.pstmt.setDate(5, new java.sql.Date(vo.getBirthday().getTime()));
		this.pstmt.setString(6, vo.getNote());
		return this.pstmt.executeUpdate() > 0 ;
	}

	@Override
	public boolean doUpdate(Member vo) throws Exception {
		String sql = "UPDATE member SET name=?,age=?,phone=?,birthday=?,note=? WHERE mid=?" ;
		this.pstmt = this.conn.prepareStatement(sql) ;
		this.pstmt.setString(1, vo.getName());
		this.pstmt.setInt(2, vo.getAge());
		this.pstmt.setString(3, vo.getPhone());
		this.pstmt.setDate(4, new java.sql.Date(vo.getBirthday().getTime()));
		this.pstmt.setString(5, vo.getNote());
		this.pstmt.setString(6, vo.getMid());
		return this.pstmt.executeUpdate() > 0 ;
	}

	@Override
	public boolean doRemoveBatch(Set<String> ids) throws Exception {
		StringBuffer buf = new StringBuffer() ;	// 需要频繁修改字符串
		buf.append("DELETE FROM member WHERE mid IN (") ;
		Iterator<String> iter = ids.iterator() ;
		while (iter.hasNext()) {
			buf.append("'").append(iter.next()).append("'").append(",") ;
		}
		buf.delete(buf.length() - 1, buf.length()).append(")") ;
		this.pstmt = this.conn.prepareStatement(buf.toString()) ;
		return this.pstmt.executeUpdate() == ids.size();
	}

	@Override
	public Member findById(String id) throws Exception {
		Member vo = null ;
		String sql = "SELECT mid,name,age,phone,birthday,note FROM member WHERE mid=?" ;
		this.pstmt = this.conn.prepareStatement(sql) ;
		this.pstmt.setString(1, id);
		ResultSet rs = this.pstmt.executeQuery() ;
		if (rs.next()) {	// 如果有数据返回则进行对象实例化
			vo = new Member() ;
			vo.setMid(rs.getString(1));
			vo.setName(rs.getString(2));
			vo.setAge(rs.getInt(3));
			vo.setPhone(rs.getString(4));
			vo.setBirthday(rs.getDate(5));
			vo.setNote(rs.getString(6));
		}
		return vo;
	}

	@Override
	public Member findByPhone(String phone) throws Exception {
		Member vo = null ;
		String sql = "SELECT mid,name,age,phone,birthday,note FROM member WHERE phone=?" ;
		this.pstmt = this.conn.prepareStatement(sql) ;
		this.pstmt.setString(1, phone);
		ResultSet rs = this.pstmt.executeQuery() ;
		if (rs.next()) {	// 如果有数据返回则进行对象实例化
			vo = new Member() ;
			vo.setMid(rs.getString(1));
			vo.setName(rs.getString(2));
			vo.setAge(rs.getInt(3));
			vo.setPhone(rs.getString(4));
			vo.setBirthday(rs.getDate(5));
			vo.setNote(rs.getString(6));
		}
		return vo;
	}

	@Override
	public List<Member> findAll() throws Exception {
		List<Member> all = new ArrayList<Member>() ;
		String sql = "SELECT mid,name,age,phone,birthday,note FROM member" ;
		this.pstmt = this.conn.prepareStatement(sql) ;
		ResultSet rs = this.pstmt.executeQuery() ;
		while (rs.next()) {
			Member vo = new Member() ;
			vo.setMid(rs.getString(1));
			vo.setName(rs.getString(2));
			vo.setAge(rs.getInt(3));
			vo.setPhone(rs.getString(4));
			vo.setBirthday(rs.getDate(5));
			vo.setNote(rs.getString(6));
			all.add(vo) ;
		}
		return all;
	}

	@Override
	public List<Member> findAllSplit(Integer currentPage, Integer lineSize) throws Exception {
		List<Member> all = new ArrayList<Member>() ;
		String sql = "SELECT * FROM ( "
				+ " 	SELECT mid,name,age,phone,birthday,note,ROWNUM rn "
				+ " 	FROM member WHERE ROWNUM<=?) temp"
				+ " WHERE temp.rn>?" ;
		this.pstmt = this.conn.prepareStatement(sql) ;
		this.pstmt.setInt(1, currentPage * lineSize);
		this.pstmt.setInt(2, (currentPage - 1) * lineSize);
		ResultSet rs = this.pstmt.executeQuery() ;
		while (rs.next()) {
			Member vo = new Member() ;
			vo.setMid(rs.getString(1));
			vo.setName(rs.getString(2));
			vo.setAge(rs.getInt(3));
			vo.setPhone(rs.getString(4));
			vo.setBirthday(rs.getDate(5));
			vo.setNote(rs.getString(6));
			all.add(vo) ;
		}
		return all;
	}

	@Override
	public List<Member> findAllSplit(String column, String keyWord, Integer currentPage, Integer lineSize)
			throws Exception {
		List<Member> all = new ArrayList<Member>() ;
		String sql = "SELECT * FROM ( "
				+ " 	SELECT mid,name,age,phone,birthday,note,ROWNUM rn "
				+ " 	FROM member WHERE "+column+" LIKE ? AND ROWNUM<=?) temp"
				+ " WHERE temp.rn>?" ;
		this.pstmt = this.conn.prepareStatement(sql) ;
		this.pstmt.setString(1, "%" + keyWord + "%");
		this.pstmt.setInt(2, currentPage * lineSize);
		this.pstmt.setInt(3, (currentPage - 1) * lineSize);
		ResultSet rs = this.pstmt.executeQuery() ;
		while (rs.next()) {
			Member vo = new Member() ;
			vo.setMid(rs.getString(1));
			vo.setName(rs.getString(2));
			vo.setAge(rs.getInt(3));
			vo.setPhone(rs.getString(4));
			vo.setBirthday(rs.getDate(5));
			vo.setNote(rs.getString(6));
			all.add(vo) ;
		}
		return all;
	}

	@Override
	public Long getAllCount() throws Exception {
		String sql = "SELECT COUNT(*) FROM member" ;
		this.pstmt = this.conn.prepareStatement(sql) ;
		ResultSet rs = this.pstmt.executeQuery() ;
		if (rs.next()) {
			return rs.getLong(1) ;
		}
		return 0L ;
	}

	@Override
	public Long getAllCount(String column, String keyWord) throws Exception {
		String sql = "SELECT COUNT(*) FROM member WHERE " + column + " LIKE ?" ;
		this.pstmt = this.conn.prepareStatement(sql) ;
		this.pstmt.setString(1, "%" + keyWord + "%");
		ResultSet rs = this.pstmt.executeQuery() ;
		if (rs.next()) {
			return rs.getLong(1) ;
		}
		return 0L ;
	}

}
