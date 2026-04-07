/**
 * 
 */
package no.hvl.dat110.middleware;

import java.math.BigInteger;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.LamportClock;
import no.hvl.dat110.util.Util;

/**
 * @author tdoy
 *
 */
public class MutualExclusion {
		
	private static final Logger logger = LogManager.getLogger(MutualExclusion.class);
	/** lock variables */
	private boolean CS_BUSY = false;						// indicate to be in critical section (accessing a shared resource) 
	private boolean WANTS_TO_ENTER_CS = false;				// indicate to want to enter CS
	private List<Message> queueack; 						// queue for acknowledged messages
	private List<Message> mutexqueue;						// queue for storing process that are denied permission. We really don't need this for quorum-protocol
	
	private LamportClock clock;								// lamport clock
	private Node node;

	private int requestClock;
	
	public MutualExclusion(Node node) throws RemoteException {
		this.node = node;
		
		clock = new LamportClock();
		queueack = new ArrayList<Message>();
		mutexqueue = new ArrayList<Message>();
	}
	
	public synchronized void acquireLock() {
		CS_BUSY = true;
	}
	
	public void releaseLocks() {
		WANTS_TO_ENTER_CS = false;
		CS_BUSY = false;
	}

	public boolean doMutexRequest(Message message, byte[] updates) throws RemoteException {
		
		logger.info(node.nodename + " wants to access CS");

		queueack.clear();

		mutexqueue.clear();

		clock.increment();

		message.setClock(clock.getClock());

		WANTS_TO_ENTER_CS = true;

		List<Message> uniquePeers = removeDuplicatePeersBeforeVoting();

		multicastMessage(message, uniquePeers);

		if (areAllMessagesReturned(uniquePeers.size())) {

			acquireLock();

			node.broadcastUpdatetoPeers(updates);

			multicastReleaseLocks(node.activenodesforfile);

			mutexqueue.clear();

			releaseLocks();

			return true;
		}

		return false;
		// clear the queueack before requesting for votes
		
		// clear the mutexqueue
		
		// increment clock
		
		// adjust the clock on the message, by calling the setClock on the message
				
		// wants to access resource - set the appropriate lock variable
	
		
		// start MutualExclusion algorithm
		
			// first, call removeDuplicatePeersBeforeVoting. A peer can hold/contain 2 replicas of a file. This peer will appear twice

			// multicast the message to activenodes (hint: use multicastMessage)
		
			// check that all replicas have replied (permission) - areAllMessagesReturned(int numvoters)?
		
			// if yes, acquireLock
		
				// send the updates to all replicas by calling node.broadcastUpdatetoPeers
		
				// clear the mutexqueue
		
		// return permission
	}
	
	// multicast message to other processes including self
	private void multicastMessage(Message message, List<Message> activenodes) throws RemoteException {
		
		logger.info("Number of peers to vote = "+activenodes.size());

		for (Message peer : activenodes) {
			NodeInterface peerStub = Util.getProcessStub(peer.getNodeName(), peer.getPort());

			peerStub.onMutexRequestReceived(message);
		}
		
		// iterate over the activenodes
		
		// obtain a stub for each node from the registry
		
		// call onMutexRequestReceived()
		
	}
	
	public void onMutexRequestReceived(Message message) throws RemoteException {

		clock.increment();

		if (message.getNodeName().equals(node.nodename)) {
			onMutexAcknowledgementReceived(message);
			return;
		}

		int caseid = -1;

		if (!CS_BUSY && !WANTS_TO_ENTER_CS) {
			caseid = 0;
		} else if (CS_BUSY) {
			caseid = 1;
		} else if (WANTS_TO_ENTER_CS) {
			caseid = 2;
		}
		
		/* write if statement to transition to the correct caseid in the doDecisionAlgorithm */
		
			// caseid=0: Receiver is not accessing shared resource and does not want to (send OK to sender)
		
			// caseid=1: Receiver already has access to the resource (dont reply but queue the request)
		
			// caseid=2: Receiver wants to access resource but is yet to - compare own message clock to received message's clock
		
		// check for decision
		doDecisionAlgorithm(message, mutexqueue, caseid);
	}
	
	public void doDecisionAlgorithm(Message message, List<Message> queue, int condition) throws RemoteException {
		
		String procName = message.getNodeName();
		int port = message.getPort();
		
		switch(condition) {
		
			/** case 1: Receiver is not accessing shared resource and does not want to (send OK to sender) */
			case 0: {
				NodeInterface senderStub = Util.getProcessStub(procName, port);

				message.setAcknowledged(true);

				senderStub.onMutexAcknowledgementReceived(message);


				// get a stub for the sender from the registry
				
				// acknowledge message
				
				// send acknowledgement back by calling onMutexAcknowledgementReceived()
				
				break;
			}
		
			/** case 2: Receiver already has access to the resource (dont reply but queue the request) */
			case 1: {
				queue.add(message);
				// queue this message
				break;
			}
			
			/**
			 *  case 3: Receiver wants to access resource but is yet to (compare own message clock to received message's clock
			 *  the message with lower timestamp wins) - send OK if received is lower. Queue message if received is higher
			 */
			case 2: {
				int receivedClock = message.getClock();

				int ownClock = node.getMessage().getClock();

				if (receivedClock < ownClock) {
					NodeInterface senderStub = Util.getProcessStub(procName, port);
					message.setAcknowledged(true);
					senderStub.onMutexAcknowledgementReceived(message);

				} else if (receivedClock == ownClock) {
					BigInteger receivedNodeID = message.getNodeID();
					BigInteger ownNodeID = node.getMessage().getNodeID();

					if (receivedNodeID.compareTo(ownNodeID) < 0) {
						NodeInterface senderStub = Util.getProcessStub(procName, port);
						message.setAcknowledged(true);
						senderStub.onMutexAcknowledgementReceived(message);
					} else {
						queue.add(message);
					}
				} else {
					queue.add(message);
				}

				// check the clock of the sending process (note that the correct clock is in the received message)
				
				// own clock of the receiver (note that the correct clock is in the node's message)
				
				// compare clocks, the lowest wins
				
				// if clocks are the same, compare nodeIDs, the lowest wins
				
				// if sender wins, acknowledge the message, obtain a stub and call onMutexAcknowledgementReceived()
				
				// if sender looses, queue it

				break;
			}
			
			default: break;
		}
		
	}
	
	public void onMutexAcknowledgementReceived(Message message) throws RemoteException {
		queueack.add(message);
		// add message to queueack
		
	}
	
	// multicast release locks message to other processes including self
	public void multicastReleaseLocks(Set<Message> activenodes) {
		logger.info("Releasing locks from = "+activenodes.size());

		for (Message peer : activenodes) {
			try {
				NodeInterface peerStub = Util.getProcessStub(peer.getNodeName(), peer.getPort());
				peerStub.releaseLocks();
			} catch (RemoteException e) {
				logger.error("Failed to release lock on peer: " + peer.getNodeName(), e);
			}
		}
		// iterate over the activenodes
		
		// obtain a stub for each node from the registry
		
		// call releaseLocks()	
	}
	
	private boolean areAllMessagesReturned(int numvoters) throws RemoteException {
		logger.info(node.getNodeName()+": size of queueack = "+queueack.size());

		if (queueack.size() == numvoters) {
			queueack.clear();
			return true;
		}

		return false;

		// check if the size of the queueack is the same as the numvoters
		
		// clear the queueack
		
		// return true if yes and false if no
	}
	
	private List<Message> removeDuplicatePeersBeforeVoting() {
		
		List<Message> uniquepeer = new ArrayList<Message>();
		for(Message p : node.activenodesforfile) {
			boolean found = false;
			for(Message p1 : uniquepeer) {
				if(p.getNodeName().equals(p1.getNodeName())) {
					found = true;
					break;
				}
			}
			if(!found)
				uniquepeer.add(p);
		}		
		return uniquepeer;
	}
}
