/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package com.twitter.test;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.thrift.*;
import org.apache.thrift.async.*;
import org.apache.thrift.meta_data.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import com.twitter.util.Future;
import com.twitter.util.Function;
import com.twitter.util.Function2;
import com.twitter.util.Try;
import com.twitter.util.Return;
import com.twitter.util.Throw;
import com.twitter.finagle.thrift.ThriftClientRequest;

public class A {

  public interface Iface {

    public int multiply(int a, int b) throws TException;

  }

  public interface AsyncIface {

    public void multiply(int a, int b, AsyncMethodCallback<AsyncClient.multiply_call> resultHandler) throws TException;

  }

  public interface ServiceIface {

    public Future<Integer> multiply(int a, int b);

  }

  public static class Client implements TServiceClient, Iface {
    public static class Factory implements TServiceClientFactory<Client> {
      public Factory() {}
      public Client getClient(TProtocol prot) {
        return new Client(prot);
      }
      public Client getClient(TProtocol iprot, TProtocol oprot) {
        return new Client(iprot, oprot);
      }
    }

    public Client(TProtocol prot)
    {
      this(prot, prot);
    }

    public Client(TProtocol iprot, TProtocol oprot)
    {
      iprot_ = iprot;
      oprot_ = oprot;
    }

    protected TProtocol iprot_;
    protected TProtocol oprot_;

    protected int seqid_;

    public TProtocol getInputProtocol()
    {
      return this.iprot_;
    }

    public TProtocol getOutputProtocol()
    {
      return this.oprot_;
    }

    public int multiply(int a, int b) throws TException
    {
      send_multiply(a, b);
      return recv_multiply();
    }

    public void send_multiply(int a, int b) throws TException
    {
      oprot_.writeMessageBegin(new TMessage("multiply", TMessageType.CALL, ++seqid_));
      multiply_args args = new multiply_args();
      args.setA(a);
      args.setB(b);
      args.write(oprot_);
      oprot_.writeMessageEnd();
      oprot_.getTransport().flush();
    }

    public int recv_multiply() throws TException
    {
      TMessage msg = iprot_.readMessageBegin();
      if (msg.type == TMessageType.EXCEPTION) {
        TApplicationException x = TApplicationException.read(iprot_);
        iprot_.readMessageEnd();
        throw x;
      }
      if (msg.seqid != seqid_) {
        throw new TApplicationException(TApplicationException.BAD_SEQUENCE_ID, "multiply failed: out of sequence response");
      }
      multiply_result result = new multiply_result();
      result.read(iprot_);
      iprot_.readMessageEnd();
      if (result.isSetSuccess()) {
        return result.success;
      }
      throw new TApplicationException(TApplicationException.MISSING_RESULT, "multiply failed: unknown result");
    }

  }
  public static class AsyncClient extends TAsyncClient implements AsyncIface {
    public static class Factory implements TAsyncClientFactory<AsyncClient> {
      private TAsyncClientManager clientManager;
      private TProtocolFactory protocolFactory;
      public Factory(TAsyncClientManager clientManager, TProtocolFactory protocolFactory) {
        this.clientManager = clientManager;
        this.protocolFactory = protocolFactory;
      }
      public AsyncClient getAsyncClient(TNonblockingTransport transport) {
        return new AsyncClient(protocolFactory, clientManager, transport);
      }
    }

    public AsyncClient(TProtocolFactory protocolFactory, TAsyncClientManager clientManager, TNonblockingTransport transport) {
      super(protocolFactory, clientManager, transport);
    }

    public void multiply(int a, int b, AsyncMethodCallback<multiply_call> resultHandler) throws TException {
      checkReady();
      multiply_call method_call = new multiply_call(a, b, resultHandler, this, protocolFactory, transport);
      manager.call(method_call);
    }

    public static class multiply_call extends TAsyncMethodCall {
      private int a;
      private int b;
      public multiply_call(int a, int b, AsyncMethodCallback<multiply_call> resultHandler, TAsyncClient client, TProtocolFactory protocolFactory, TNonblockingTransport transport) throws TException {
        super(client, protocolFactory, transport, resultHandler, false);
        this.a = a;
        this.b = b;
      }

      public void write_args(TProtocol prot) throws TException {
        prot.writeMessageBegin(new TMessage("multiply", TMessageType.CALL, 0));
        multiply_args args = new multiply_args();
        args.setA(a);
        args.setB(b);
        args.write(prot);
        prot.writeMessageEnd();
      }

      public int getResult() throws TException {
        if (getState() != State.RESPONSE_READ) {
          throw new IllegalStateException("Method call not finished!");
        }
        TMemoryInputTransport memoryTransport = new TMemoryInputTransport(getFrameBuffer().array());
        TProtocol prot = client.getProtocolFactory().getProtocol(memoryTransport);
        return (new Client(prot)).recv_multiply();
      }
    }

  }

  public static class ServiceToClient implements ServiceIface {
    private com.twitter.finagle.Service<ThriftClientRequest, byte[]> service;
    private TProtocolFactory protocolFactory;
    public ServiceToClient(com.twitter.finagle.Service<ThriftClientRequest, byte[]> service, TProtocolFactory protocolFactory) {
      this.service = service;
      this.protocolFactory = protocolFactory;
    }

    public Future<Integer> multiply(int a, int b) {
      try {
        // TODO: size
        TMemoryBuffer __memoryTransport__ = new TMemoryBuffer(512);
        TProtocol __prot__ = this.protocolFactory.getProtocol(__memoryTransport__);
        __prot__.writeMessageBegin(new TMessage("multiply", TMessageType.CALL, 0));
        multiply_args __args__ = new multiply_args();
        __args__.setA(a);
        __args__.setB(b);
        __args__.write(__prot__);
        __prot__.writeMessageEnd();
      

        byte[] __buffer__ = Arrays.copyOfRange(__memoryTransport__.getArray(), 0, __memoryTransport__.length());
        ThriftClientRequest __request__ = new ThriftClientRequest(__buffer__, false);
        Future<byte[]> __done__ = this.service.apply(__request__);
        return __done__.flatMap(new Function<byte[], Try<Integer>>() {
          public Future<Integer> apply(byte[] __buffer__) {
            TMemoryInputTransport __memoryTransport__ = new TMemoryInputTransport(__buffer__);
            TProtocol __prot__ = ServiceToClient.this.protocolFactory.getProtocol(__memoryTransport__);
            try {
              return Future.value((new Client(__prot__)).recv_multiply());
            } catch (Exception e) {
              return Future.exception(e);
            }
          }
        });
      } catch (TException e) {
        return Future.exception(e);
      }
    }
  }

  public static class Processor implements TProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(Processor.class.getName());
    public Processor(Iface iface)
    {
      iface_ = iface;
      processMap_.put("multiply", new multiply());
    }

    protected static interface ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException;
    }

    private Iface iface_;
    protected final HashMap<String,ProcessFunction> processMap_ = new HashMap<String,ProcessFunction>();

    public boolean process(TProtocol iprot, TProtocol oprot) throws TException
    {
      TMessage msg = iprot.readMessageBegin();
      ProcessFunction fn = processMap_.get(msg.name);
      if (fn == null) {
        TProtocolUtil.skip(iprot, TType.STRUCT);
        iprot.readMessageEnd();
        TApplicationException x = new TApplicationException(TApplicationException.UNKNOWN_METHOD, "Invalid method name: '"+msg.name+"'");
        oprot.writeMessageBegin(new TMessage(msg.name, TMessageType.EXCEPTION, msg.seqid));
        x.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
        return true;
      }
      fn.process(msg.seqid, iprot, oprot);
      return true;
    }

    private class multiply implements ProcessFunction {
      public void process(int seqid, TProtocol iprot, TProtocol oprot) throws TException
      {
        multiply_args args = new multiply_args();
        try {
          args.read(iprot);
        } catch (TProtocolException e) {
          iprot.readMessageEnd();
          TApplicationException x = new TApplicationException(TApplicationException.PROTOCOL_ERROR, e.getMessage());
          oprot.writeMessageBegin(new TMessage("multiply", TMessageType.EXCEPTION, seqid));
          x.write(oprot);
          oprot.writeMessageEnd();
          oprot.getTransport().flush();
          return;
        }
        iprot.readMessageEnd();
        multiply_result result = new multiply_result();
        result.success = iface_.multiply(args.a, args.b);
        result.setSuccessIsSet(true);
        oprot.writeMessageBegin(new TMessage("multiply", TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
      }

    }

  }

  public static class Service extends com.twitter.finagle.Service<byte[], byte[]> {
    private final ServiceIface iface;
    private final TProtocolFactory protocolFactory;
    protected HashMap<String, Function2<TProtocol, Integer, Future<byte[]>>> functionMap = new HashMap<String, Function2<TProtocol, Integer, Future<byte[]>>>();
    public Service(final ServiceIface iface, final TProtocolFactory protocolFactory) {
      this.iface = iface;
      this.protocolFactory = protocolFactory;
      functionMap.put("multiply", new Function2<TProtocol, Integer, Future<byte[]>>() {
        public Future<byte[]> apply(final TProtocol iprot, final Integer seqid) {
          multiply_args args = new multiply_args();
          try {
            args.read(iprot);
          } catch (TProtocolException e) {
            try {
              iprot.readMessageEnd();
              TApplicationException x = new TApplicationException(TApplicationException.PROTOCOL_ERROR, e.getMessage());
              TMemoryBuffer memoryBuffer = new TMemoryBuffer(512);
              TProtocol oprot = protocolFactory.getProtocol(memoryBuffer);
          
              oprot.writeMessageBegin(new TMessage("multiply", TMessageType.EXCEPTION, seqid));
              x.write(oprot);
              oprot.writeMessageEnd();
              oprot.getTransport().flush();
              byte[] buffer = Arrays.copyOfRange(memoryBuffer.getArray(), 0, memoryBuffer.length());
              return Future.value(buffer);
            } catch (Exception e1) {
              return Future.exception(e1);
            }
          } catch (Exception e) {
            return Future.exception(e);
          }
          
          try {
            iprot.readMessageEnd();
          } catch (Exception e) {
            return Future.exception(e);
          }
          Future<Integer> future;
          try {
            future = iface.multiply(args.a, args.b);
          } catch (Exception e) {
            future = Future.exception(e);
          }
          try {
            return future.flatMap(new Function<Integer, Try<byte[]>>() {
              public Future<byte[]> apply(Integer value) {
                multiply_result result = new multiply_result();
                result.success = value;
                result.setSuccessIsSet(true);
          
                try {
                  TMemoryBuffer memoryBuffer = new TMemoryBuffer(512);
                  TProtocol oprot = protocolFactory.getProtocol(memoryBuffer);
                   
                  oprot.writeMessageBegin(new TMessage("multiply", TMessageType.REPLY, seqid));
                  result.write(oprot);
                  oprot.writeMessageEnd();
                   
                  return Future.value(Arrays.copyOfRange(memoryBuffer.getArray(), 0, memoryBuffer.length()));
                } catch (Exception e) {
                  return Future.exception(e);
                }
              }
            }).rescue(new Function<Throwable, Try<byte[]>>() {
              public Try<byte[]> apply(Throwable t) {
                TMemoryBuffer memoryBuffer = new TMemoryBuffer(512);
                TProtocol oprot = protocolFactory.getProtocol(memoryBuffer);
                try {
                  TApplicationException x = new TApplicationException(TApplicationException.INTERNAL_ERROR, "Internal error processing multiply");
                  oprot.writeMessageBegin(new TMessage("multiply", TMessageType.EXCEPTION, seqid));
                  x.write(oprot);
                  oprot.writeMessageEnd();
                  oprot.getTransport().flush();
                  return Future.value(Arrays.copyOfRange(memoryBuffer.getArray(), 0, memoryBuffer.length()));
                } catch (Exception e) {
                  return Future.exception(e);
                }
              }
            });
          } catch (Exception e) {
            return Future.exception(e);
          }
        }
      });
      
    }
    
    public Future<byte[]> apply(byte[] request) {
      TTransport inputTransport = new TMemoryInputTransport(request);
      TProtocol iprot = protocolFactory.getProtocol(inputTransport);
    
      TMessage msg;
      try {
        msg = iprot.readMessageBegin();
      } catch (Exception e) {
        return Future.exception(e);
      }
    
      Function2<TProtocol, Integer, Future<byte[]>> fn = functionMap.get(msg.name);
      if (fn == null) {
        try {
          TProtocolUtil.skip(iprot, TType.STRUCT);
          iprot.readMessageEnd();
          TApplicationException x = new TApplicationException(TApplicationException.UNKNOWN_METHOD, "Invalid method name: '"+msg.name+"'");
          TMemoryBuffer memoryBuffer = new TMemoryBuffer(512);
          TProtocol oprot = protocolFactory.getProtocol(memoryBuffer);
          oprot.writeMessageBegin(new TMessage(msg.name, TMessageType.EXCEPTION, msg.seqid));
          x.write(oprot);
          oprot.writeMessageEnd();
          oprot.getTransport().flush();
          return Future.value(Arrays.copyOfRange(memoryBuffer.getArray(), 0, memoryBuffer.length()));
        } catch (Exception e) {
          return Future.exception(e);
        }
      }
    
      return fn.apply(iprot, msg.seqid);
    }

  }

  public static class multiply_args implements TBase<multiply_args, multiply_args._Fields>, java.io.Serializable, Cloneable   {
    private static final TStruct STRUCT_DESC = new TStruct("multiply_args");

    private static final TField A_FIELD_DESC = new TField("a", TType.I32, (short)1);
    private static final TField B_FIELD_DESC = new TField("b", TType.I32, (short)2);

    public int a;
    public int b;

    /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
    public enum _Fields implements TFieldIdEnum {
      A((short)1, "a"),
      B((short)2, "b");

      private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

      static {
        for (_Fields field : EnumSet.allOf(_Fields.class)) {
          byName.put(field.getFieldName(), field);
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, or null if its not found.
       */
      public static _Fields findByThriftId(int fieldId) {
        switch(fieldId) {
          case 1: // A
            return A;
          case 2: // B
            return B;
          default:
            return null;
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, throwing an exception
       * if it is not found.
       */
      public static _Fields findByThriftIdOrThrow(int fieldId) {
        _Fields fields = findByThriftId(fieldId);
        if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
        return fields;
      }

      /**
       * Find the _Fields constant that matches name, or null if its not found.
       */
      public static _Fields findByName(String name) {
        return byName.get(name);
      }

      private final short _thriftId;
      private final String _fieldName;

      _Fields(short thriftId, String fieldName) {
        _thriftId = thriftId;
        _fieldName = fieldName;
      }

      public short getThriftFieldId() {
        return _thriftId;
      }

      public String getFieldName() {
        return _fieldName;
      }
    }

    // isset id assignments
    private static final int __A_ISSET_ID = 0;
    private static final int __B_ISSET_ID = 1;
    private BitSet __isset_bit_vector = new BitSet(2);

    public static final Map<_Fields, FieldMetaData> metaDataMap;
    static {
      Map<_Fields, FieldMetaData> tmpMap = new EnumMap<_Fields, FieldMetaData>(_Fields.class);
      tmpMap.put(_Fields.A, new FieldMetaData("a", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.I32)));
      tmpMap.put(_Fields.B, new FieldMetaData("b", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.I32)));
      metaDataMap = Collections.unmodifiableMap(tmpMap);
      FieldMetaData.addStructMetaDataMap(multiply_args.class, metaDataMap);
    }

    public multiply_args() {
    }

    public multiply_args(
      int a,
      int b)
    {
      this();
      this.a = a;
      setAIsSet(true);
      this.b = b;
      setBIsSet(true);
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public multiply_args(multiply_args other) {
      __isset_bit_vector.clear();
      __isset_bit_vector.or(other.__isset_bit_vector);
      this.a = other.a;
      this.b = other.b;
    }

    public multiply_args deepCopy() {
      return new multiply_args(this);
    }

    @Override
    public void clear() {
      setAIsSet(false);
      this.a = 0;
      setBIsSet(false);
      this.b = 0;
    }

    public int getA() {
      return this.a;
    }

    public multiply_args setA(int a) {
      this.a = a;
      setAIsSet(true);
      return this;
    }

    public void unsetA() {
      __isset_bit_vector.clear(__A_ISSET_ID);
    }

    /** Returns true if field a is set (has been asigned a value) and false otherwise */
    public boolean isSetA() {
      return __isset_bit_vector.get(__A_ISSET_ID);
    }

    public void setAIsSet(boolean value) {
      __isset_bit_vector.set(__A_ISSET_ID, value);
    }

    public int getB() {
      return this.b;
    }

    public multiply_args setB(int b) {
      this.b = b;
      setBIsSet(true);
      return this;
    }

    public void unsetB() {
      __isset_bit_vector.clear(__B_ISSET_ID);
    }

    /** Returns true if field b is set (has been asigned a value) and false otherwise */
    public boolean isSetB() {
      return __isset_bit_vector.get(__B_ISSET_ID);
    }

    public void setBIsSet(boolean value) {
      __isset_bit_vector.set(__B_ISSET_ID, value);
    }

    public void setFieldValue(_Fields field, Object value) {
      switch (field) {
      case A:
        if (value == null) {
          unsetA();
        } else {
          setA((Integer)value);
        }
        break;

      case B:
        if (value == null) {
          unsetB();
        } else {
          setB((Integer)value);
        }
        break;

      }
    }

    public Object getFieldValue(_Fields field) {
      switch (field) {
      case A:
        return new Integer(getA());

      case B:
        return new Integer(getB());

      }
      throw new IllegalStateException();
    }

    /** Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise */
    public boolean isSet(_Fields field) {
      if (field == null) {
        throw new IllegalArgumentException();
      }

      switch (field) {
      case A:
        return isSetA();
      case B:
        return isSetB();
      }
      throw new IllegalStateException();
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof multiply_args)
        return this.equals((multiply_args)that);
      return false;
    }

    public boolean equals(multiply_args that) {
      if (that == null)
        return false;

      boolean this_present_a = true;
      boolean that_present_a = true;
      if (this_present_a || that_present_a) {
        if (!(this_present_a && that_present_a))
          return false;
        if (this.a != that.a)
          return false;
      }

      boolean this_present_b = true;
      boolean that_present_b = true;
      if (this_present_b || that_present_b) {
        if (!(this_present_b && that_present_b))
          return false;
        if (this.b != that.b)
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public int compareTo(multiply_args other) {
      if (!getClass().equals(other.getClass())) {
        return getClass().getName().compareTo(other.getClass().getName());
      }

      int lastComparison = 0;
      multiply_args typedOther = (multiply_args)other;

      lastComparison = Boolean.valueOf(isSetA()).compareTo(typedOther.isSetA());
      if (lastComparison != 0) {
        return lastComparison;
      }
      if (isSetA()) {
        lastComparison = TBaseHelper.compareTo(this.a, typedOther.a);
        if (lastComparison != 0) {
          return lastComparison;
        }
      }
      lastComparison = Boolean.valueOf(isSetB()).compareTo(typedOther.isSetB());
      if (lastComparison != 0) {
        return lastComparison;
      }
      if (isSetB()) {
        lastComparison = TBaseHelper.compareTo(this.b, typedOther.b);
        if (lastComparison != 0) {
          return lastComparison;
        }
      }
      return 0;
    }

    public _Fields fieldForId(int fieldId) {
      return _Fields.findByThriftId(fieldId);
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id) {
          case 1: // A
            if (field.type == TType.I32) {
              this.a = iprot.readI32();
              setAIsSet(true);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          case 2: // B
            if (field.type == TType.I32) {
              this.b = iprot.readI32();
              setBIsSet(true);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(A_FIELD_DESC);
      oprot.writeI32(this.a);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(B_FIELD_DESC);
      oprot.writeI32(this.b);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("multiply_args(");
      boolean first = true;

      sb.append("a:");
      sb.append(this.a);
      first = false;
      if (!first) sb.append(", ");
      sb.append("b:");
      sb.append(this.b);
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
    }

  }

  public static class multiply_result implements TBase<multiply_result, multiply_result._Fields>, java.io.Serializable, Cloneable   {
    private static final TStruct STRUCT_DESC = new TStruct("multiply_result");

    private static final TField SUCCESS_FIELD_DESC = new TField("success", TType.I32, (short)0);

    public int success;

    /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
    public enum _Fields implements TFieldIdEnum {
      SUCCESS((short)0, "success");

      private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

      static {
        for (_Fields field : EnumSet.allOf(_Fields.class)) {
          byName.put(field.getFieldName(), field);
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, or null if its not found.
       */
      public static _Fields findByThriftId(int fieldId) {
        switch(fieldId) {
          case 0: // SUCCESS
            return SUCCESS;
          default:
            return null;
        }
      }

      /**
       * Find the _Fields constant that matches fieldId, throwing an exception
       * if it is not found.
       */
      public static _Fields findByThriftIdOrThrow(int fieldId) {
        _Fields fields = findByThriftId(fieldId);
        if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
        return fields;
      }

      /**
       * Find the _Fields constant that matches name, or null if its not found.
       */
      public static _Fields findByName(String name) {
        return byName.get(name);
      }

      private final short _thriftId;
      private final String _fieldName;

      _Fields(short thriftId, String fieldName) {
        _thriftId = thriftId;
        _fieldName = fieldName;
      }

      public short getThriftFieldId() {
        return _thriftId;
      }

      public String getFieldName() {
        return _fieldName;
      }
    }

    // isset id assignments
    private static final int __SUCCESS_ISSET_ID = 0;
    private BitSet __isset_bit_vector = new BitSet(1);

    public static final Map<_Fields, FieldMetaData> metaDataMap;
    static {
      Map<_Fields, FieldMetaData> tmpMap = new EnumMap<_Fields, FieldMetaData>(_Fields.class);
      tmpMap.put(_Fields.SUCCESS, new FieldMetaData("success", TFieldRequirementType.DEFAULT, 
          new FieldValueMetaData(TType.I32)));
      metaDataMap = Collections.unmodifiableMap(tmpMap);
      FieldMetaData.addStructMetaDataMap(multiply_result.class, metaDataMap);
    }

    public multiply_result() {
    }

    public multiply_result(
      int success)
    {
      this();
      this.success = success;
      setSuccessIsSet(true);
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public multiply_result(multiply_result other) {
      __isset_bit_vector.clear();
      __isset_bit_vector.or(other.__isset_bit_vector);
      this.success = other.success;
    }

    public multiply_result deepCopy() {
      return new multiply_result(this);
    }

    @Override
    public void clear() {
      setSuccessIsSet(false);
      this.success = 0;
    }

    public int getSuccess() {
      return this.success;
    }

    public multiply_result setSuccess(int success) {
      this.success = success;
      setSuccessIsSet(true);
      return this;
    }

    public void unsetSuccess() {
      __isset_bit_vector.clear(__SUCCESS_ISSET_ID);
    }

    /** Returns true if field success is set (has been asigned a value) and false otherwise */
    public boolean isSetSuccess() {
      return __isset_bit_vector.get(__SUCCESS_ISSET_ID);
    }

    public void setSuccessIsSet(boolean value) {
      __isset_bit_vector.set(__SUCCESS_ISSET_ID, value);
    }

    public void setFieldValue(_Fields field, Object value) {
      switch (field) {
      case SUCCESS:
        if (value == null) {
          unsetSuccess();
        } else {
          setSuccess((Integer)value);
        }
        break;

      }
    }

    public Object getFieldValue(_Fields field) {
      switch (field) {
      case SUCCESS:
        return new Integer(getSuccess());

      }
      throw new IllegalStateException();
    }

    /** Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise */
    public boolean isSet(_Fields field) {
      if (field == null) {
        throw new IllegalArgumentException();
      }

      switch (field) {
      case SUCCESS:
        return isSetSuccess();
      }
      throw new IllegalStateException();
    }

    @Override
    public boolean equals(Object that) {
      if (that == null)
        return false;
      if (that instanceof multiply_result)
        return this.equals((multiply_result)that);
      return false;
    }

    public boolean equals(multiply_result that) {
      if (that == null)
        return false;

      boolean this_present_success = true;
      boolean that_present_success = true;
      if (this_present_success || that_present_success) {
        if (!(this_present_success && that_present_success))
          return false;
        if (this.success != that.success)
          return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    public int compareTo(multiply_result other) {
      if (!getClass().equals(other.getClass())) {
        return getClass().getName().compareTo(other.getClass().getName());
      }

      int lastComparison = 0;
      multiply_result typedOther = (multiply_result)other;

      lastComparison = Boolean.valueOf(isSetSuccess()).compareTo(typedOther.isSetSuccess());
      if (lastComparison != 0) {
        return lastComparison;
      }
      if (isSetSuccess()) {
        lastComparison = TBaseHelper.compareTo(this.success, typedOther.success);
        if (lastComparison != 0) {
          return lastComparison;
        }
      }
      return 0;
    }

    public _Fields fieldForId(int fieldId) {
      return _Fields.findByThriftId(fieldId);
    }

    public void read(TProtocol iprot) throws TException {
      TField field;
      iprot.readStructBegin();
      while (true)
      {
        field = iprot.readFieldBegin();
        if (field.type == TType.STOP) { 
          break;
        }
        switch (field.id) {
          case 0: // SUCCESS
            if (field.type == TType.I32) {
              this.success = iprot.readI32();
              setSuccessIsSet(true);
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
          default:
            TProtocolUtil.skip(iprot, field.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      validate();
    }

    public void write(TProtocol oprot) throws TException {
      oprot.writeStructBegin(STRUCT_DESC);

      if (this.isSetSuccess()) {
        oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
        oprot.writeI32(this.success);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("multiply_result(");
      boolean first = true;

      sb.append("success:");
      sb.append(this.success);
      first = false;
      sb.append(")");
      return sb.toString();
    }

    public void validate() throws TException {
      // check for required fields
    }

  }

}
