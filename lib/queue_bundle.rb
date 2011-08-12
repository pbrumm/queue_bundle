require "queue_bundle/version"

require 'thread'

class QueueNotFound < StandardError; end

class QueueBundle
  

  
  attr_reader :hash_algorithm
  def initialize(size, options = {})
    @mutex = Mutex.new
    @queues = []
    @size = size
    @closed = false
    @hash_algorithm = options[:hash_algorithm] || :simple_hash
    @key_lookup = options[:key_lookup] || :id
    1.upto(@size) {|i|
      @queues << Queue.new
    }
  end
  def is_queue?
    true
  end
  def simple_hash(key)
    key.hash % @size
  end
  def simple_index(key)
    key
  end
  
 # def resize(new_size)
 #   
 # end

  def closed?
    @closed
  end
  def empty?(index = nil)
    self.length(index) == 0
  end
  def reassign(index)
    @mutex.synchronize do
      dest_queue = next_queue(index)
      if !dest_queue.nil?
        old_queue = @queues[index]
        @queues[index] = :reassigned
        if !old_queue.empty?
          begin
            while !old_queue.empty?
              dest_queue.push(old_queue.pop(true))
            end
          rescue ThreadError
            
          end
          
        end
        true
      else
        false
      end
    end
  end

  def close()
    @closed = true
  end
  def push(obj, key = nil)
    key ||= get_key(obj)
    queue = get_queue(key)
    
    if queue.nil?
      raise QueueNotFound.new("Queue not found for #{key}")
    else
      queue.push(obj)
    end
  end
  alias_method :enq, :push
  def pop(index, non_block = false)
    queue = @queues[index]
    
    if queue.nil? 
      raise QueueNotFound.new("Queue not found for #{key}")
    elsif queue == :reassigned
      raise QueueNotFound.new("Queue reassigned")
    else
      queue.pop(non_block)
    end
  end
  alias_method :deq, :pop
  def clear(index = nil)
    if index.nil?
      active_queues.each {|queue| queue.clear}
    else
      queue = @queues[index]   #doesn't switch to next queue so that it will error out
      
      if queue.nil? 
        raise QueueNotFound.new("Queue not found for #{key}")
      elsif queue == :reassigned
        raise QueueNotFound.new("Queue reassigned")
      else
        queue.clear()
      end
    end
  end
  def num_waiting(index = nil)
    if index.nil?
      active_queues.map {|queue| queue.num_waiting}.inject{|sum,x| sum + x }
    else
      queue = @queues[index]
      
      if queue.nil? 
        raise QueueNotFound.new("Queue not found for #{key}")
      elsif queue == :reassigned
        raise QueueNotFound.new("Queue reassigned")
      else
        queue.num_waiting
      end
    end
  end
  def empty?(index = nil)
    if index.nil?
      active_queues.inject{|total,y|  (!total || !y) ? false : true }  # if it sees a false then it is not empty
    else
      queue = @queues[index]
      
      if queue.nil? 
        raise QueueNotFound.new("Queue not found for #{key}")
      elsif queue == :reassigned
        raise QueueNotFound.new("Queue reassigned")
      else
        queue.empty?
      end
    end
  end
  
  def length(index = nil)
    if index.nil?
      active_queues.map {|queue| queue.length}.inject{|sum,x| sum + x }
    else
      queue = @queues[index]
      
      if queue.nil? 
        raise QueueNotFound.new("Queue not found for #{key}")
      elsif queue == :reassigned
        raise QueueNotFound.new("Queue reassigned")
      else
        queue.length
      end
    end
  end
  
  alias_method :size, :length
  
  def lengths
    length_hash = {}
    active_queues.each_index {|queue_index| 
      length_hash[queue_index] = @queues[queue_index].length
    }
    length_hash
  end
  alias_method :sizes, :lengths
  private
  
    def get_key(obj)
      key = nil
      if @key_lookup.kind_of?(Proc)
        key = @key_lookup.call obj
      elsif @key_lookup.kind_of?(Symbol)
        if obj.kind_of?(Hash)
          key = obj[@key_lookup]
        else
          key = obj.send(@key_lookup)
        end
      end
      key
    end
    def get_queue(key)
      queue_index = nil
      if @hash_algorithm.kind_of?(Proc)
        queue_index = @hash_algorithm.call key
      elsif @hash_algorithm.kind_of?(Symbol)
        queue_index = self.send(@hash_algorithm, key)
      end
      queue = @queues[queue_index]
      if queue == :reassigned
        queue = next_queue(queue_index)
      end
      
      queue
    end
    def active_queues
      @queues.select {|queue| queue != :reassigned }
    end
    def next_queue(index)
      current_index = index
      while n = next_index(current_index) 
        break if n == index
        if @queues[n] != :reassigned
          return @queues[n]
        end
        current_index = n
      end
      nil
    end
    def next_index(index)
      n = index + 1
      if n >= @size
        n = 0
      end
      n
    end
end