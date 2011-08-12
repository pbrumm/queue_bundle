$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
$LOAD_PATH.unshift(File.dirname(__FILE__))
require 'helper'
require 'ostruct'

class TestQueueBundle < Test::Unit::TestCase

  def test_simple_index
    queue = QueueBundle.new(5, :hash_algorithm => :simple_index)
    assert_equal :simple_index, queue.hash_algorithm
    assert_raise(QueueNotFound) do
       queue.push(1, 50)
    end
    0.upto(4) {|i|
      queue.push(i, i)
    }
    assert_equal 5, queue.length
    0.upto(4) {|i|
      assert_equal 1, queue.length(i)
    }
    0.upto(4) {|i|
      assert_equal i, queue.pop(i) 
      assert_equal 0, queue.length(i)
      assert_raise(ThreadError) do
        queue.pop(i, true)
      end
      
    }
    assert_equal 0, queue.length
    assert_equal false, queue.closed?
    queue.close
    assert_equal true, queue.closed?
  end
  
  def test_simple_hash
    queue = QueueBundle.new(5, :hash_algorithm => :simple_hash)
    assert_equal :simple_hash, queue.hash_algorithm
   
    0.upto(4) {|i|
      queue.push(i, i)
    }
    
    assert_equal 5, queue.length
    results = []
    0.upto(4) {|i|
      1.upto(queue.length(i)) {
        results << queue.pop(i)
      }
    }
    assert_equal [0,1,2,3,4], results.sort
    assert_equal 0, queue.length
    assert_equal false, queue.closed?
    queue.close
    assert_equal true, queue.closed?
  end
  
  def test_simple_index_with_hash_key
    queue = QueueBundle.new(5, :hash_algorithm => :simple_index)
    assert_equal :simple_index, queue.hash_algorithm
    assert_raise(QueueNotFound) do
       queue.push(1, 50)
    end
    0.upto(4) {|i|
      queue.push({:id => i})
    }
    assert_equal 5, queue.length
    0.upto(4) {|i|
      assert_equal 1, queue.length(i)
    }
    0.upto(4) {|i|
      assert_equal i, queue.pop(i)[:id]
      assert_equal 0, queue.length(i)
      assert_raise(ThreadError) do
        queue.pop(i, true)
      end
      
    }
    assert_equal 0, queue.length
    assert_equal false, queue.closed?
    queue.close
    assert_equal true, queue.closed?
  end
  
  def test_simple_index_with_hash_alt_key
    queue = QueueBundle.new(5, :hash_algorithm => :simple_index, :key_lookup => :alt)
    assert_equal :simple_index, queue.hash_algorithm
    assert_raise(QueueNotFound) do
       queue.push(1, 50)
    end
    0.upto(4) {|i|
      queue.push({:alt => i})
    }
    assert_equal 5, queue.length
    0.upto(4) {|i|
      assert_equal 1, queue.length(i)
    }
    0.upto(4) {|i|
      assert_equal i, queue.pop(i)[:alt]
      assert_equal 0, queue.length(i)
      assert_raise(ThreadError) do
        queue.pop(i, true)
      end
      
    }
    assert_equal 0, queue.length
    assert_equal false, queue.closed?
    queue.close
    assert_equal true, queue.closed?
  end

  def test_simple_index_with_obj_alt_key
    queue = QueueBundle.new(5, :hash_algorithm => :simple_index, :key_lookup => :alt)
    assert_equal :simple_index, queue.hash_algorithm
    assert_raise(QueueNotFound) do
       queue.push(1, 50)
    end
    0.upto(4) {|i|
      queue.push(OpenStruct.new({:alt => i}))
    }
    assert_equal 5, queue.length
    0.upto(4) {|i|
      assert_equal 1, queue.length(i)
    }
    0.upto(4) {|i|
      assert_equal i, queue.pop(i).alt
      assert_equal 0, queue.length(i)
      assert_raise(ThreadError) do
        queue.pop(i, true)
      end
      
    }
    assert_equal 0, queue.length
    assert_equal false, queue.closed?
    queue.close
    assert_equal true, queue.closed?
  end

  def test_simple_index_with_reassign
    queue = QueueBundle.new(5, :hash_algorithm => :simple_index)
    assert_equal :simple_index, queue.hash_algorithm
    assert_raise(QueueNotFound) do
       queue.push(1, 50)
    end
    0.upto(4) {|i|
      queue.push(i, i)
    }
    assert_equal 5, queue.length
    0.upto(4) {|i|
      assert_equal 1, queue.length(i)
    }
    assert_equal true, queue.reassign(4)
    assert_equal 2, queue.length(0)  #wrapped
    1.upto(3) {|i|
      assert_equal 1, queue.length(i)
    }
    assert_raise(QueueNotFound) do
        queue.pop(4, true)
      end
    assert_equal 0, queue.pop(0) 
    assert_equal 4, queue.pop(0) 
    
    1.upto(3) {|i|
      assert_equal i, queue.pop(i) 
      assert_equal 0, queue.length(i)
      assert_raise(ThreadError) do
        queue.pop(i, true)
      end
      
    }
    
    assert_equal 0, queue.length
    queue.push(4,4)
    assert_equal 1, queue.length(0)
    
    assert_equal false, queue.closed?
    queue.close
    assert_equal true, queue.closed?
    
    
  end
  
  def test_simple_index_with_reassign_all
    queue = QueueBundle.new(5, :hash_algorithm => :simple_index)
    assert_equal :simple_index, queue.hash_algorithm
    assert_raise(QueueNotFound) do
       queue.push(1, 50)
    end
    0.upto(4) {|i|
      queue.push(i, i)
    }
    assert_equal true, queue.reassign(3)
    assert_equal 2, queue.size(4)
    assert_equal true, queue.reassign(4)
    assert_equal 3, queue.size(0)
    assert_equal true, queue.reassign(0)
    assert_equal 4, queue.size(1)
    assert_equal true, queue.reassign(1)
    assert_equal 5, queue.size(2)
    assert_equal false, queue.reassign(2)

    
    assert_equal 2, queue.pop(2)
    assert_equal 1, queue.pop(2)
    assert_equal 0, queue.pop(2)
    assert_equal 4, queue.pop(2)
    assert_equal 3, queue.pop(2)

 
  end
  def test_doc_example
    queue = QueueBundle.new(5, :key_lookup => :id)
    threads = []
    0.upto(4).each {|worker_id|
      threads << Thread.new do 
        loop do
          break if queue.closed? && queue.empty?(worker_id)
          work = queue.pop(worker_id)   #in this case this would be blocking
        end
      end
    }
    0.upto(50).each {|work_id|
      queue.push({id: work_id, name: "task"})
    }
    
    queue.close
    
    threads.compact.each do |t|
      begin
        t.join
      rescue Interrupt
      end
    end
  end
  
end

