The QueueBundle presents itself as a standard queue to those who put(value) stuff in.
the value is then hashed based on some simple hashing algorithm's,  or a custom one can be provided.
based on the result of that hashing it is slotted into an output queue

threads can then query for results from their queue by specifying their index.


Install
=======
    sudo gem install queue_bundle

Usage
=====

    #A simple approach for usimg would be
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
    # sets a flag that queue is closed so threads know that no more work is coming
    queue.close    
    
    threads.each do |t|
      begin
        t.join
      rescue Interrupt
      end
    end